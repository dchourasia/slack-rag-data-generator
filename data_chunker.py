"""
Step 3 – Split the sanitized text into token-bounded chunks and write each
chunk to a separate text file.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import tiktoken
from langchain_text_splitters import TokenTextSplitter

logger = logging.getLogger(__name__)


def _count_tokens(text: str, encoding_name: str) -> int:
    enc = tiktoken.get_encoding(encoding_name)
    return len(enc.encode(text))


def chunk_data(processed_data_dir: str, config: dict | None = None) -> str:
    """
    Read the single sanitized text file, split it into token-bounded chunks,
    and write each chunk to a separate file.

    Parameters
    ----------
    processed_data_dir : str
        Path to the ``slack_processed_data_<ts>`` directory from Step 2.
    config : dict, optional
        Parsed config.yaml contents.

    Returns
    -------
    str
        Absolute path to the ``slack_output_sources_<ts>`` directory.
    """
    if config is None:
        import yaml
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

    chunk_cfg = config.get("chunking", {})
    max_tokens = chunk_cfg.get("max_tokens_per_chunk", 50000)
    chunk_overlap = chunk_cfg.get("chunk_overlap", 200)
    encoding_name = chunk_cfg.get("encoding_name", "cl100k_base")

    proc_path = Path(processed_data_dir)
    text_file = proc_path / "all_messages.txt"
    if not text_file.exists():
        raise FileNotFoundError(f"all_messages.txt not found in {proc_path}")

    full_text = text_file.read_text(encoding="utf-8")
    if not full_text.strip():
        logger.warning("Input text is empty; nothing to chunk.")
        return str(proc_path)

    splitter = TokenTextSplitter(
        encoding_name=encoding_name,
        chunk_size=max_tokens,
        chunk_overlap=chunk_overlap,
    )
    chunks = splitter.split_text(full_text)

    base_dir = Path(config["output"]["base_dir"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = base_dir / f"slack_output_sources_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    chunk_details: list[dict] = []
    for idx, chunk_text in enumerate(chunks, start=1):
        filename = f"chunk_{idx:03d}.txt"
        filepath = out_dir / filename
        filepath.write_text(chunk_text, encoding="utf-8")

        token_count = _count_tokens(chunk_text, encoding_name)
        chunk_details.append({
            "filename": filename,
            "token_count": token_count,
            "char_count": len(chunk_text),
        })

    summary = {
        "source_dir": str(proc_path.resolve()),
        "total_chunks": len(chunks),
        "encoding_name": encoding_name,
        "max_tokens_per_chunk": max_tokens,
        "chunk_overlap": chunk_overlap,
        "chunks": chunk_details,
    }
    with open(out_dir / "chunk_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(
        "Chunking complete – %d chunks written to %s", len(chunks), out_dir
    )
    return str(out_dir.resolve())
