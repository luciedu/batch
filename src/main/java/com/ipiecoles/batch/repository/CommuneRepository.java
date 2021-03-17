package com.ipiecoles.batch.repository;
import com.ipiecoles.batch.model.Commune;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CommuneRepository extends JpaRepository<Commune, String> {
    @Query("select count(distinct c.codePostal) from Commune c")
    long countDistinctCodePostal();

    @Query("select count(distinct c.nom) from Commune c")
    long countDistinctNom();
}
