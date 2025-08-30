from __future__ import annotations

from pydantic_settings import SettingsConfigDict, BaseSettings

class DQManagerConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')
