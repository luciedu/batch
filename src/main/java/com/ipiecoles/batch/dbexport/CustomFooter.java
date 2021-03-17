package com.ipiecoles.batch.dbexport;



import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.beans.factory.annotation.Value;
import java.io.IOException;
import java.io.Writer;


public class CustomFooter implements FlatFileFooterCallback {

    private final CommuneRepository communeRepository;

    public CustomFooter(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    @Override
    public void writeFooter(Writer writer) throws IOException {
        writer.write("Total communes : " + communeRepository.countDistinctNom());
    }

}