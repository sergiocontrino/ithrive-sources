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
import org.intermine.objectstore.ObjectStoreException;
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

    private Map<String, Item> patients = new HashMap<>();

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

            Item patient = createPatient(patientId,ethnicity,gender);
//            Item patient = createItem("Patient");
//            //patient.setReference("organism", getOrganism(HUMAN_TAXON));
//            patient.setAttribute("identifier", patientId);
//            patient.setAttribute("ethnicity", ethnicity);
//            patient.setAttribute("gender", gender);

            Item referral = createItem("Referral");
            referral.setAttribute("patientAge", age);
            referral.setAttribute("identifier", referralId);
            referral.setReference("patient", patient);

            //store(patient);
            store(referral);
        }

        for (Item item : patients.values()) {
            Integer pid = store(item);
        }
    }



    private Item createPatient(String patientId, String ethnicity, String gender)
            throws ObjectStoreException {
        Item item = patients.get(patientId);
        if (item == null) {
            item = createItem("Patient");
            item.setAttribute("identifier", patientId);
            if (!ethnicity.isEmpty()) {
                item.setAttribute("ethnicity", ethnicity);
            }
            if (!gender.isEmpty()) {
                item.setAttribute("gender", gender);
            }
            patients.put(patientId, item);
        }
        return item;
    }




}
