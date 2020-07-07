package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2019 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import org.apache.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;

import java.io.Reader;
import java.util.*;


/**
 * 
 * @author
 */
public class NhsConverter extends BioFileConverter
{
    //
    private static final String DATASET_TITLE = "Districts reports";
    private static final String DATA_SOURCE_NAME = "NHS";
    private static final String HUMAN_TAXON = "9606";
    protected static final Logger LOG = Logger.getLogger(NhsConverter.class);

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public NhsConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE, null);
    }

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {

        Set<String> duplicateEnsembls = new HashSet<String>();
        Map<String, Integer> storedGeneIds = new HashMap<String, Integer>();
        Map<String, String> geneEnsemblIds = new HashMap<String, String>();

        // Read all lines into id pairs, track any ensembl ids or symbols that appear twice
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // Period,Patient ID,Referral ID,Age at date of referral,EthnicityDescription,Gender
        // e.g.
        // 2015-04-01-2019-03-31,1021297,4,17,White - British,M
        // 2015-04-01-2019-03-31,1098489,3,16,White - British,M
        // 015-04-01-2019-03-31,1108035,4,13,White - Irish,M

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("ZZ " + header.toString());

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

//            if (line[0].startsWith("GENE_RGD_ID")) {
//                continue;
//            }
            String period = line[0];
            String patientId = line[1];
            String referralId = line[2];
            String age = line[3];
            String ethnicity = line[4];
            String gender = line[5];

            LOG.info("PAT: " + patientId);

            Item patient = createItem("Patient");
            //patient.setReference("organism", getOrganism(HUMAN_TAXON));
            patient.setAttribute("identifier", patientId);
            patient.setAttribute("ethnicity", ethnicity);
            patient.setAttribute("gender", gender);

            Item referral = createItem("Referral");
            referral.setAttribute("patientAge", age);
            referral.setAttribute("identifier", referralId);
            referral.setReference("patient", patient);

            store(patient);
            store(referral);

//            Set<String> ensemblIds = parseEnsemblIds(ensembl);
//            for (String ensemblId : ensemblIds) {
//                createCrossReference(patient.getIdentifier(), ensemblId, "Ensembl", true);
//            }

//            if (!StringUtils.isBlank(name)) {
//                patient.setAttribute("name", name);
//            }
//            if (!StringUtils.isBlank(description)) {
//                patient.setAttribute("description", description);
//            }
//            if (!StringUtils.isBlank(entrez)) {
//                createCrossReference(patient.getIdentifier(), entrez, "NCBI", true);
//            }
//
//            Integer storedGeneId = store(patient);
//            storedGeneIds.put(patient.getIdentifier(), storedGeneId);


        }

//        LOG.info("ENSEMBL: duplicateEnsemblIds.size() = " + duplicateEnsembls.size());
//        LOG.info("ENSEMBL: duplicateEnsemblIds = " + duplicateEnsembls);
//        // now check that we only saw each ensembl id once
//        for (Map.Entry<String, String> entry : geneEnsemblIds.entrySet()) {
//            String geneIdentifier = entry.getKey();
//            String ensemblId = entry.getValue();
//            if (!duplicateEnsembls.contains(ensemblId)) {
//                Attribute att = new Attribute("primaryIdentifier", ensemblId);
//                store(att, storedGeneIds.get(geneIdentifier));
//            }
//        }

    }

//    private Set<String> parseEnsemblIds(String fromFile) {
//        Set<String> ensembls = new HashSet<String>();
//        if (!StringUtils.isBlank(fromFile)) {
//            ensembls.addAll(Arrays.asList(fromFile.split(";")));
//        }
//        return ensembls;
//    }

}
