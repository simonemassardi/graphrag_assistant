import os
import json
from pathlib import Path
from loguru import logger

from langchain.document_loaders import PyPDFLoader


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

class Ingest:
    def __init__(self):
        self.raw_dir = RAW_DIR
        self.processed_dir = PROCESSED_DIR

    def parse_pdf(self, file_path: Path) -> None:
        try:
            loader = PyPDFLoader(file_path)
            docs = loader.load()
            return docs
        except Exception as e:
            logger.error(f"Error parsing PDF: {e}")
            return None
        
    def ingest_wasde_reports(self) -> None:
        for pdf_file in (self.raw_dir / "wasde").glob("*.pdf"):
            docs = self.parse_pdf(pdf_file)
            if docs:
                logger.info(f"Ingesting {pdf_file} with {len(docs)} documents")
                with open(self.processed_dir / f"{pdf_file.stem}.json", "w") as f:
                    for doc in docs:
                        metadata = {
                            "source": "wasde",
                            "file_name": pdf_file.name,
                        }
                        f.write(json.dumps({
                            **metadata,
                            "text": doc.page_content
                        }) + "\n")


if __name__ == "__main__":
    ingest = Ingest()
    ingest.ingest_wasde_reports()