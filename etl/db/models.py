from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class Match(Base):
    __tablename__ = "Match"
    
    id = Column(String(50), primary_key=True)
    startTime = Column(DateTime, nullable=False)
    endTime = Column(DateTime, nullable=True)
    tournamentId = Column(String(100), nullable=True)
    createdAt = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    damage_events = relationship("DamageEvent", back_populates="match")
    elim_events = relationship("EliminationEvent", back_populates="match")
    players = relationship("Player", back_populates="match")


class Player(Base):
    __tablename__ = "Player"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    epicId = Column(String(100), nullable=False)
    epicUsername = Column(String(100), nullable=False)
    matchId = Column(String(50), ForeignKey("Match.id"), nullable=False)
    
    # Relationships
    match = relationship("Match", back_populates="players")
    damage_dealt = relationship("DamageEvent", foreign_keys="DamageEvent.shooterId", back_populates="shooter")
    damage_taken = relationship("DamageEvent", foreign_keys="DamageEvent.victimId", back_populates="victim")
    
    __table_args__ = (
        Index('idx_player_epic_match', 'epicId', 'matchId', unique=True),
        Index('idx_player_match', 'matchId'),
    )


class DamageEvent(Base):
    __tablename__ = "DamageEvent"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    matchId = Column(String(50), ForeignKey("Match.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    gameTimeSeconds = Column(Integer, nullable=False)
    
    # Players
    shooterId = Column(String(100), nullable=False)
    victimId = Column(String(100), nullable=False)
    
    # Damage details
    weaponId = Column(String(100), nullable=False)
    weaponType = Column(String(50), nullable=True)
    damageAmount = Column(Float, nullable=False)
    
    # Positions
    shooterX = Column(Float, nullable=False)
    shooterY = Column(Float, nullable=False)
    shooterZ = Column(Float, nullable=False)
    victimX = Column(Float, nullable=False)
    victimY = Column(Float, nullable=False)
    victimZ = Column(Float, nullable=False)
    distance = Column(Float, nullable=False)
    
    # Context
    zone = Column(Integer, nullable=False)
    
    # Relationships
    match = relationship("Match", back_populates="damage_events")
    shooter = relationship("Player", foreign_keys=[shooterId], back_populates="damage_dealt")
    victim = relationship("Player", foreign_keys=[victimId], back_populates="damage_taken")
    
    __table_args__ = (
        Index('idx_damage_match', 'matchId'),
        Index('idx_damage_shooter', 'shooterId'),
        Index('idx_damage_victim', 'victimId'),
        Index('idx_damage_weapon', 'weaponType'),
        Index('idx_damage_zone', 'zone'),
        Index('idx_damage_distance', 'distance'),
        Index('idx_damage_time', 'gameTimeSeconds'),
    )


class EliminationEvent(Base):
    __tablename__ = "EliminationEvent"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    matchId = Column(String(50), ForeignKey("Match.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    gameTimeSeconds = Column(Integer, nullable=False)
    
    # Players
    actorId = Column(String(100), nullable=False)
    victimId = Column(String(100), nullable=False)
    
    # Elimination details
    weaponId = Column(String(100), nullable=False)
    weaponType = Column(String(50), nullable=True)
    
    # Positions
    actorX = Column(Float, nullable=False)
    actorY = Column(Float, nullable=False)
    actorZ = Column(Float, nullable=False)
    victimX = Column(Float, nullable=False)
    victimY = Column(Float, nullable=False)
    victimZ = Column(Float, nullable=False)
    distance = Column(Float, nullable=False)
    
    # Context
    zone = Column(Integer, nullable=False)
    
    # Relationships
    match = relationship("Match", back_populates="elim_events")
    
    __table_args__ = (
        Index('idx_elim_match', 'matchId'),
        Index('idx_elim_actor', 'actorId'),
        Index('idx_elim_victim', 'victimId'),
        Index('idx_elim_zone', 'zone'),
    )


# Database connection
def get_engine():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not set in .env")
    return create_engine(database_url, echo=False)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    """Create all tables"""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✅ Database tables created")
