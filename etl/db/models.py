from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timezone
import os
from dotenv import load_dotenv


load_dotenv()

Base = declarative_base()


class EventWindow(Base):
    __tablename__ = "event_windows"

    event_window_id = Column(String(100), primary_key=True) # eventWindowId
    processing = Column(Boolean, nullable=False, default=False)
    processed = Column(Boolean, nullable=False, default=False)
    failed = Column(Boolean, nullable=False, default=False)

    discovered_at = Column(DateTime, default=datetime.now(timezone.utc))
    last_processing_start = Column(DateTime)
    last_processed = Column(DateTime)
    last_failed = Column(DateTime)

    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)

    # relationships
    matches = relationship("Match", back_populates="event_window")

    def __repr__(self):
        return f"<EventWindow(event_window_id={self.event_window_id})>"


class Match(Base):
    __tablename__ = "matches"
    
    match_id = Column(String(50), primary_key=True)

    event_window_id = Column(String(50), ForeignKey("event_windows.event_window_id"), nullable=False)
    event_id = Column(String(100), nullable=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    gamemode = Column(String(100), nullable=True)
    duration = Column(DateTime, nullable=True)
    player_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
    
    # Relationships
    event_window = relationship("EventWindow", back_populates="matches")
    damage_dealt_events = relationship("DamageDealtEvent", back_populates="match")
    elim_events = relationship("EliminationEvent", back_populates="match")
    players = relationship("MatchPlayer", back_populates="match")

    def __repr__(self):
        return f"<Match(match_id={self.match_id})>"


class MatchPlayer(Base):
    __tablename__ = "match_players"
    
    id = Column(Integer, primary_key=True, autoincrement=True)

    epic_id = Column(String(100), nullable=False)
    epic_username = Column(String(100), nullable=False)
    match_id = Column(String(50), ForeignKey("matches.match_id"), nullable=False)

    # Relationships
    match = relationship("Match", back_populates="players")
    damage_dealt = relationship(
        "DamageDealtEvent",
        foreign_keys="DamageDealtEvent.actor_id", 
        back_populates="actor"
    )
    damage_taken = relationship(
        "DamageDealtEvent", 
        foreign_keys="DamageDealtEvent.recipient_id", 
        back_populates="recipient"
    )
    
    __table_args__ = (
        Index('idx_player_epic_match', 'epic_id', 'match_id', unique=True),
        Index('idx_player_match', 'match_id'),
    )

    def __repr__(self):
        return f"<MatchPlayer(epic_id={self.epic_id}, epic_username={self.epic_username}, match_id={self.match_id})>"


class DamageDealtEvent(Base):
    __tablename__ = "damage_dealt_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String(50), ForeignKey("matches.match_id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    game_time_seconds = Column(Integer, nullable=False)
    
    actor_id = Column(Integer, ForeignKey("match_players.id"), nullable=False)
    recipient_id = Column(Integer, ForeignKey("match_players.id"), nullable=False)
    
    weapon_id = Column(String(100), nullable=False)
    weapon_type = Column(String(50), nullable=True)
    damage_amount = Column(Float, nullable=False)
    
    actor_x = Column(Float, nullable=False)
    actor_y = Column(Float, nullable=False)
    actor_z = Column(Float, nullable=False)
    recipient_x = Column(Float, nullable=False)
    recipient_y = Column(Float, nullable=False)
    recipient_z = Column(Float, nullable=False)
    distance = Column(Float, nullable=False)
    
    zone = Column(Integer, nullable=False)

    match = relationship("Match", back_populates="damage_dealt_events")
    actor = relationship(
        "MatchPlayer", 
        foreign_keys=[actor_id], 
        back_populates="damage_dealt"
    )
    recipient = relationship(
        "MatchPlayer", 
        foreign_keys=[recipient_id], 
        back_populates="damage_taken"
    )
    
    __table_args__ = (
        Index('idx_damage_match', 'match_id'),
        Index('idx_damage_actor', 'actor_id'),
        Index('idx_damage_recipient', 'recipient_id'),
        Index('idx_damage_weapon', 'weapon_type'),
        Index('idx_damage_zone', 'zone'),
        Index('idx_damage_distance', 'distance'),
        Index('idx_damage_time', 'game_time_seconds'),
    )

    def __repr__(self):
        return f"<DamageDealtEvent(actor_id={self.actor_id}, recipient_id={self.recipient_id}, damage_amount={self.damage_amount}, match_id={self.match_id})>"


class EliminationEvent(Base):
    __tablename__ = "elimination_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String(50), ForeignKey("matches.match_id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    game_time_seconds = Column(Integer, nullable=False)
    
    actor_id = Column(Integer, ForeignKey("match_players.id"), nullable=False)
    recipient_id = Column(Integer, ForeignKey("match_players.id"), nullable=False)
    
    weapon_id = Column(String(100), nullable=False)
    weapon_type = Column(String(50), nullable=True)
    
    actor_x = Column(Float, nullable=False)
    actor_y = Column(Float, nullable=False)
    actor_z = Column(Float, nullable=False)
    recipient_x = Column(Float, nullable=False)
    recipient_y = Column(Float, nullable=False)
    recipient_z = Column(Float, nullable=False)
    distance = Column(Float, nullable=False)
    
    zone = Column(Integer, nullable=False)
    
    match = relationship("Match", back_populates="elim_events")
    
    __table_args__ = (
        Index('idx_elim_match', 'match_id'),
        Index('idx_elim_actor', 'actor_id'),
        Index('idx_elim_recipient', 'recipient_id'),
        Index('idx_elim_zone', 'zone'),
    )

    def __repr__(self):
        return f"<EliminationEvent(actor_id={self.actor_id}, recipient_id={self.recipient_id}, match_id={self.match_id})>"


# Database connection
def get_engine():
    database_url = os.getenv("DATABASE_URL")
    print(f"DATABASE_URL: {database_url}")
    if not database_url:
        raise ValueError("DATABASE_URL not set in .env")
    return create_engine(database_url, echo=False)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def reinit_db():
    """Create all tables"""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("✅ Database tables created")

if __name__ == "__main__":
    init_db()
