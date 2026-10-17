from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import lru_cache
from urllib.parse import unquote, urlparse

MODEL_ID = "onnx-community/embeddinggemma-300m-ONNX"
MODEL_REVISION = "5090578d9565bb06545b4552f76e6bc2c93e4a66"
MODEL_FILENAME = "onnx/model_q4.onnx"
MODEL_DATA_FILENAME = "onnx/model_q4.onnx_data"
MODEL_OUTPUT_NAME = "sentence_embedding"
PREFIX = "task: clustering | query: "
DEFAULT_THRESHOLD = 0.7

CATEGORY_DESCRIPTIONS = {
    "AI & Machine Learning": (
        "Artificial intelligence and machine learning engineering: neural "
        "networks, large language models, generative AI, model training, "
        "inference, embeddings, computer vision, natural language processing, "
        "reinforcement learning, AI agents, model evaluation and applied AI "
        "systems."
    ),
    "Software Engineering": (
        "Software engineering and application development: software "
        "architecture, design patterns, testing, debugging, APIs, frameworks, "
        "libraries, developer tools, source control, code quality, software "
        "maintenance and application design."
    ),
    "Programming Languages": (
        "Programming languages and language technology: Python, Rust, Go, "
        "JavaScript, C++, language syntax, type systems, compilers, "
        "interpreters, language runtimes, programming paradigms and language "
        "features."
    ),
    "Data & Databases": (
        "Data engineering, storage and databases: SQL, NoSQL, PostgreSQL, "
        "MongoDB, vector databases, data warehouses, data lakes, ETL, data "
        "pipelines, analytics, indexing, query optimization and data "
        "processing."
    ),
    "Cloud & DevOps": (
        "Cloud infrastructure and DevOps: AWS, Azure, Google Cloud, Docker, "
        "Kubernetes, containers, CI/CD, infrastructure as code, Terraform, "
        "deployment, monitoring, observability, site reliability engineering "
        "and cloud operations."
    ),
    "Security & Privacy": (
        "Cybersecurity, information security and privacy: vulnerabilities, "
        "exploits, malware, ransomware, authentication, encryption, "
        "cryptography, network security, application security, privacy "
        "protection and data breaches."
    ),
    "Hardware & Systems": (
        "Computer hardware and low-level systems: CPUs, GPUs, processors, "
        "memory, storage devices, semiconductors, computer architecture, "
        "operating systems, Linux kernels, device drivers, embedded systems "
        "and hardware performance."
    ),
    "Science & Research": (
        "Scientific research and discoveries: physics, mathematics, chemistry, "
        "biology, medicine, astronomy, scientific experiments, academic "
        "studies, research methodology and discoveries outside applied AI "
        "engineering."
    ),
    "Startups & Business": (
        "Technology startups and business: company funding, venture capital, "
        "investments, acquisitions, IPOs, business strategy, entrepreneurship, "
        "revenue, technology markets, product launches and corporate earnings."
    ),
    "Society & Policy": (
        "Technology and society: government regulation, public policy, AI "
        "legislation, digital rights, technology ethics, social impacts of "
        "technology, employment, education, copyright policy and geopolitical "
        "technology issues."
    ),
}

Embedding = Sequence[float]
Encoder = Callable[[list[str]], Sequence[Embedding]]


class OnnxEmbeddingEncoder:
    """Load the Q4 EmbeddingGemma model and return normalized embeddings."""

    def __init__(self) -> None:
        """Download cached model assets and initialize CPU inference."""
        import numpy as np
        import onnxruntime as ort
        from huggingface_hub import hf_hub_download
        from transformers import AutoTokenizer

        model_path = hf_hub_download(
            repo_id=MODEL_ID,
            filename=MODEL_FILENAME,
            revision=MODEL_REVISION,
        )
        hf_hub_download(
            repo_id=MODEL_ID,
            filename=MODEL_DATA_FILENAME,
            revision=MODEL_REVISION,
        )

        self._np = np
        self._tokenizer = AutoTokenizer.from_pretrained(
            MODEL_ID,
            revision=MODEL_REVISION,
        )
        self._session = ort.InferenceSession(
            model_path,
            providers=["CPUExecutionProvider"],
        )
        self._input_names = {item.name for item in self._session.get_inputs()}

        output_names = {item.name for item in self._session.get_outputs()}
        if MODEL_OUTPUT_NAME not in output_names:
            raise ValueError(
                f"ONNX model does not expose {MODEL_OUTPUT_NAME!r} output"
            )

    def __call__(self, texts: list[str]):
        """Encode prefixed texts and L2-normalize their sentence embeddings."""
        inputs = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="np",
        )
        feeds = {
            name: value
            for name, value in inputs.items()
            if name in self._input_names
        }
        embeddings = self._session.run([MODEL_OUTPUT_NAME], feeds)[0]
        norms = self._np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings / self._np.maximum(norms, 1e-12)


class TopicClassifier:
    """Classify Hacker News publications by embedding similarity."""

    def __init__(
        self,
        threshold: float = DEFAULT_THRESHOLD,
        encoder: Encoder | None = None,
    ) -> None:
        """Prepare category embeddings once for subsequent classifications."""
        self.threshold = threshold
        self.categories = tuple(CATEGORY_DESCRIPTIONS)
        self._encoder = encoder or OnnxEmbeddingEncoder()
        self.category_embeddings = self._encode(
            list(CATEGORY_DESCRIPTIONS.values())
        )

    def _encode(self, texts: list[str]) -> Sequence[Embedding]:
        """Apply the symmetric clustering prompt and encode all texts."""
        return self._encoder([PREFIX + text for text in texts])

    def classify_topic(self, title: str, url: str | None = None) -> str:
        """Return the nearest category or Other below the fixed threshold."""
        if not title.strip():
            return "Other"

        text = title.strip()
        if url:
            parsed = urlparse(url)
            domain = parsed.hostname or ""
            path = unquote(parsed.path).replace("-", " ").replace("_", " ")
            text += f"\nSource: {domain}\nURL path: {path[:200]}"

        embedding = self._encode([text])[0]
        similarities = [
            sum(
                float(left) * float(right)
                for left, right in zip(row, embedding, strict=True)
            )
            for row in self.category_embeddings
        ]
        best_idx = max(range(len(similarities)), key=similarities.__getitem__)
        if similarities[best_idx] < self.threshold:
            return "Other"
        return self.categories[best_idx]


@lru_cache(maxsize=1)
def _get_classifier() -> TopicClassifier:
    """Create one classifier per Airflow task process and reuse its model."""
    return TopicClassifier()


def classify_topic(title: str, url: str | None) -> str:
    """Classify one publication through the process-local classifier."""
    return _get_classifier().classify_topic(title, url)

