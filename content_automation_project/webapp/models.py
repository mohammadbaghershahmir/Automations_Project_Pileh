import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from webapp.database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    jobs = relationship("Job", back_populates="created_by_user")
    inbox_notifications = relationship("InboxNotification", back_populates="user")


class Job(Base):
    __tablename__ = "jobs"

    id = Column(String(36), primary_key=True, default=_uuid)
    type = Column(String(32), default="test_bank")
    status = Column(String(32), default="draft")
    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    error_summary = Column(Text, nullable=True)
    config_json = Column(Text, default="{}")
    cancel_requested = Column(Boolean, default=False, nullable=False)

    created_by_user = relationship("User", back_populates="jobs")
    pairs = relationship("JobPair", back_populates="job", cascade="all, delete-orphan")
    artifacts = relationship("Artifact", back_populates="job", cascade="all, delete-orphan")
    log_lines = relationship("JobLogLine", back_populates="job", cascade="all, delete-orphan")


class JobPair(Base):
    __tablename__ = "job_pairs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    pair_index = Column(Integer, nullable=False)
    stage_j_filename = Column(String(512), nullable=False)
    word_filename = Column(String(512), nullable=True)
    stage_j_relpath = Column(String(1024), nullable=False)
    word_relpath = Column(String(1024), nullable=True)
    step1_status = Column(String(32), default="pending")
    step2_status = Column(String(32), default="pending")
    step1_error = Column(Text, nullable=True)
    step2_error = Column(Text, nullable=True)
    output_relpath = Column(String(1024), default="")

    job = relationship("Job", back_populates="pairs")


class Artifact(Base):
    __tablename__ = "artifacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    pair_index = Column(Integer, nullable=True)
    rel_path = Column(String(2048), nullable=False)
    role = Column(String(64), default="file")
    byte_size = Column(BigInteger, default=0)
    sha256 = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    job = relationship("Job", back_populates="artifacts")


class JobLogLine(Base):
    __tablename__ = "job_log_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    seq = Column(Integer, nullable=False, index=True)
    ts = Column(DateTime, default=datetime.utcnow)
    line = Column(Text, nullable=False)
    pair_index = Column(Integer, nullable=True)

    job = relationship("Job", back_populates="log_lines")


class InboxNotification(Base):
    """Per-user inbox row for job completion / failure (worker writes, UI reads)."""

    __tablename__ = "inbox_notifications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    kind = Column(String(32), nullable=False)
    title = Column(String(512), nullable=False)
    body = Column(Text, nullable=True)
    read_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    user = relationship("User", back_populates="inbox_notifications")
