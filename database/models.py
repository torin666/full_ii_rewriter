from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Source:
    id: Optional[int]
    user_id: int
    source_url: str
    theme: str
    created_at: datetime
    is_active: bool = True

    @classmethod
    def create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source_url TEXT NOT NULL,
            theme TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE(user_id, source_url)
        )
        """ 