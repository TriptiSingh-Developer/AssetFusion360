
'''
AssetFusion360 | Lightweight ONNX Embedding Engine

CPU-based MiniLM inference without PyTorch
or Sentence Transformers runtime dependencies.
'''

import numpy as np
import onnxruntime as ort

from tokenizers import Tokenizer
from huggingface_hub import hf_hub_download


MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


class MiniLMONNXEmbedder:

    def __init__(self):

        model_path = hf_hub_download(
            repo_id=MODEL_NAME,
            filename="onnx/model.onnx"
        )

        tokenizer_path = hf_hub_download(
            repo_id=MODEL_NAME,
            filename="tokenizer.json"
        )

        self.tokenizer = Tokenizer.from_file(
            tokenizer_path
        )

        self.tokenizer.enable_truncation(
            max_length=256
        )

        self.tokenizer.enable_padding(
            direction="right",
            pad_id=0,
            pad_token="[PAD]"
        )

        options = ort.SessionOptions()

        options.intra_op_num_threads = 2
        options.inter_op_num_threads = 1

        options.graph_optimization_level = (
            ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        )

        self.session = ort.InferenceSession(

            model_path,

            sess_options=options,

            providers=["CPUExecutionProvider"]

        )

    def encode(self, texts):

        if isinstance(texts, str):
            texts = [texts]

        encoded = self.tokenizer.encode_batch(
            texts
        )

        input_ids = np.asarray(
            [item.ids for item in encoded],
            dtype=np.int64
        )

        attention_mask = np.asarray(
            [item.attention_mask for item in encoded],
            dtype=np.int64
        )

        token_type_ids = np.asarray(
            [item.type_ids for item in encoded],
            dtype=np.int64
        )

        available_inputs = {

            "input_ids": input_ids,

            "attention_mask": attention_mask,

            "token_type_ids": token_type_ids

        }

        inputs = {

            item.name: available_inputs[item.name]

            for item in self.session.get_inputs()

        }

        token_embeddings = self.session.run(
            None,
            inputs
        )[0]

        # Attention-mask-aware mean pooling.

        mask = attention_mask[
            :, :, None
        ].astype(np.float32)

        pooled = (
            token_embeddings * mask
        ).sum(axis=1)

        pooled = pooled / np.maximum(
            mask.sum(axis=1),
            1e-9
        )

        # L2 normalization.

        normalized = pooled / np.maximum(

            np.linalg.norm(
                pooled,
                axis=1,
                keepdims=True
            ),

            1e-12

        )

        return np.ascontiguousarray(
            normalized,
            dtype=np.float32
        )


if __name__ == "__main__":

    model = MiniLMONNXEmbedder()

    embeddings = model.encode([

        "Leaking water pipe",

        "Broken playground equipment",

        "Toilet unable to flush"

    ])

    print("Embedding shape:", embeddings.shape)

    print(
        "Vector norms:",
        np.linalg.norm(embeddings, axis=1)
    )

    print("ONNX EMBEDDING ENGINE: SUCCESS")
