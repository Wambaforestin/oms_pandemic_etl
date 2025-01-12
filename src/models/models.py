from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Text, TIMESTAMP
from sqlalchemy.orm import relationship

Base = declarative_base()

class Pays(Base):
   __tablename__ = 'pays'
   __table_args__ = {'extend_existing': True}
   
   id_pays = Column(Integer, primary_key=True)
   nom_pays = Column(String(100), nullable=False, unique=True)
   code_iso = Column(String(3), unique=True)
   region_oms = Column(String(50))

   # Relation avec epidemie_pays
   epidemies = relationship("EpidemiePays", back_populates="pays")

class Maladie(Base):
   __tablename__ = 'maladie'
   __table_args__ = {'extend_existing': True}
   
   id_maladie = Column(Integer, primary_key=True)
   nom_maladie = Column(String(50), nullable=False, unique=True)
   description = Column(Text)
   taux_mortalite_moyen = Column(Float)
   date_premiere_apparition = Column(Date)
   organisation_surveillance = Column(String(50))

   # Relation avec epidemie_pays
   epidemies = relationship("EpidemiePays", back_populates="maladie")

class EpidemiePays(Base):
   __tablename__ = 'epidemie_pays'
   __table_args__ = {'extend_existing': True}
   
   id_epidemie = Column(Integer, primary_key=True)
   date_premier_cas = Column(Date, nullable=False)
   date_fin = Column(Date)
   statut = Column(String(50), nullable=False)
   mesures_prises = Column(Text)
   id_maladie = Column(Integer, ForeignKey('maladie.id_maladie'), nullable=False)
   id_pays = Column(Integer, ForeignKey('pays.id_pays'), nullable=False)

   # Relations
   pays = relationship("Pays", back_populates="epidemies")
   maladie = relationship("Maladie", back_populates="epidemies")
   statistiques = relationship("StatistiquesQuotidiennes", back_populates="epidemie")

class StatistiquesQuotidiennes(Base):
   __tablename__ = 'statistiques_quotidiennes'
   __table_args__ = {'extend_existing': True}
   
   id_stat = Column(Integer, primary_key=True)
   date_observation = Column(Date, nullable=False)
   cas_total = Column(Integer, default=0)
   deces_total = Column(Integer, default=0)
   nouveaux_cas = Column(Integer, default=0)
   nouveaux_deces = Column(Integer, default=0)
   cas_actifs = Column(Integer, default=0)
   cas_gueris = Column(Integer, default=0)
   id_epidemie = Column(Integer, ForeignKey('epidemie_pays.id_epidemie'), nullable=False)

   # Relations
   epidemie = relationship("EpidemiePays", back_populates="statistiques")
   stats_detaillees = relationship("StatistiquesDetaillees", back_populates="stat_quotidienne", uselist=False)

class StatistiquesDetaillees(Base):
   __tablename__ = 'statistiques_detaillees'
   __table_args__ = {'extend_existing': True}
   
   id_stat_detail = Column(Integer, primary_key=True)
   cas_par_million = Column(Float, default=0)
   deces_par_million = Column(Float, default=0)
   moyenne_mobile_cas = Column(Float, default=0)
   moyenne_mobile_deces = Column(Float, default=0)
   id_stat = Column(Integer, ForeignKey('statistiques_quotidiennes.id_stat'), nullable=False, unique=True)

   # Relation
   stat_quotidienne = relationship("StatistiquesQuotidiennes", back_populates="stats_detaillees")