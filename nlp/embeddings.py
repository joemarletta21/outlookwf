from __future__ import annotations

import os
import json
from typing import List, Tuple, Optional


class EmbeddingIndexer:
    """Manages a local FAISS index for message embeddings.

    - Uses SentenceTransformers for 2k-char excerpts.
    - Stores FAISS index at `index_path` and an ID mapping sidecar at `index_path + '.meta.jsonl'`.
    - Adds in batches to limit memory while ingesting.
    """

    def __init__(self, model_name: str, index_path: str):
        self.model_name = model_name
        self.index_path = index_path
        self.meta_path = index_path + ".meta.jsonl"
        self._model = None
        self._index = None
        self._dim = None
        self._buffer: List[Tuple[int, List[float], str]] = []  # (message_id, vector, excerpt)

    def _ensure_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer  # type: ignore
                self._model = SentenceTransformer(self.model_name)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to load embeddings model '{self.model_name}'. Install/cache it locally. Error: {e}"
                )

    def _ensure_index(self):
        if self._index is not None:
            return
        try:
            import faiss  # type: ignore
        except Exception as e:
            raise RuntimeError("FAISS not available. Install faiss-cpu. Error: %s" % e)

        if os.path.exists(self.index_path):
            self._index = faiss.read_index(self.index_path)
            self._dim = self._index.d
        else:
            # Create an inner-product flat index (use normalized vectors for cosine similarity)
            self._dim = self.embed_dim()
            self._index = faiss.IndexFlatIP(self._dim)

    def embed_dim(self) -> int:
        self._ensure_model()
        # Run a tiny forward to discover dimension if unknown
        if self._dim is not None:
            return self._dim
        vec = self._model.encode(["dim probe"], normalize_embeddings=True)
        self._dim = int(vec.shape[1])
        return self._dim

    def encode_excerpt(self, subject: str, body: str) -> Tuple[List[float], str]:
        self._ensure_model()
        text = (subject or "").strip() + "\n\n" + (body or "").strip()
        excerpt = text[:2000]
        vec = self._model.encode([excerpt], normalize_embeddings=True)
        return vec[0].tolist(), excerpt

    def add(self, message_id: int, subject: str, body: str):
        try:
            vec, excerpt = self.encode_excerpt(subject, body)
        except Exception as e:
            # Model not available or failed; skip silently but could log
            return
        self._buffer.append((message_id, vec, excerpt))
        if len(self._buffer) >= 500:
            self.flush()

    def flush(self):
        if not self._buffer:
            return
        self._ensure_index()
        import numpy as np  # type: ignore
        import faiss  # type: ignore

        ids = [mid for (mid, _v, _ex) in self._buffer]
        mat = np.array([v for (_mid, v, _ex) in self._buffer], dtype="float32")
        self._index.add(mat)
        # append meta mapping lines
        os.makedirs(os.path.dirname(self.meta_path) or ".", exist_ok=True)
        with open(self.meta_path, "a", encoding="utf-8") as f:
            for mid, _v, ex in self._buffer:
                f.write(json.dumps({"message_id": mid, "excerpt": ex}) + "\n")
        # persist index
        os.makedirs(os.path.dirname(self.index_path) or ".", exist_ok=True)
        faiss.write_index(self._index, self.index_path)
        self._buffer.clear()

    def search(self, query: str, k: int = 10) -> List[Tuple[int, float]]:
        self._ensure_index()
        self._ensure_model()
        import numpy as np  # type: ignore
        q = self._model.encode([query], normalize_embeddings=True)
        D, I = self._index.search(np.array(q, dtype="float32"), k)
        # Map FAISS local ids to message_ids using meta file order
        msg_ids = self._load_meta_ids()
        results: List[Tuple[int, float]] = []
        for idx, score in zip(I[0], D[0]):
            if idx == -1:
                continue
            if idx < len(msg_ids):
                results.append((msg_ids[idx], float(score)))
        return results

    def _load_meta_ids(self) -> List[int]:
        ids: List[int] = []
        if not os.path.exists(self.meta_path):
            return ids
        with open(self.meta_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    ids.append(int(obj.get("message_id")))
                except Exception:
                    continue
        return ids

