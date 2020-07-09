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

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;


/**
 * @author
 */
public class NhsConverter extends BioFileConverter {
    //
    private static final String DATASET_TITLE = "Districts reports";
    private static final String DATA_SOURCE_NAME = "NHS";
    protected static final Logger LOG = Logger.getLogger(NhsConverter.class);

    private Map<String, Item> patients = new HashMap<>();   // patientId, patient
    private Map<String, Item> referrals = new HashMap<>();  // patRefId, referral

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public NhsConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE, null);
    }

    @Override
    public void process(Reader reader) throws Exception {
        File f = getCurrentFile();
        String fileName = getCurrentFile().getName();
        if (fileName.endsWith("csv")) {
            LOG.info("Reading file: " + fileName);
            if (fileName.equalsIgnoreCase("campet.csv"))
                processDemographic(new FileReader(f));
            if (fileName.equalsIgnoreCase("campetPatLevDia.csv"))
                processDiagnosis(new FileReader(f));
            if (fileName.equalsIgnoreCase("campetPatLevCon.csv"))
                processContact(new FileReader(f));
        }

//        storeReferrals();
//        storePatients();
    }


    private void processDemographic(Reader reader) throws Exception {

//        Set<String> duplicateEnsembls = new HashSet<String>();
//        Map<String, Integer> storedGeneIds = new HashMap<String, Integer>();
//        Map<String, String> geneEnsemblIds = new HashMap<String, String>();

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
        LOG.info("PROC DEMO " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String period = line[0];
            String patientId = line[1];
            String referralId = line[2];
            String age = line[3];
            String ethnicity = line[4];
            String gender = line[5];

            String patRefId = patientId + "-" + referralId;  // to identify the referral
            //LOG.info("PAT: " + patRefId);

            Item patient = createPatient(patientId, ethnicity, gender);
            Item referral = createReferral(patRefId, referralId, age, patient);
        }

        //storeReferrals();
        storePatients();

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

    private Item createReferral (String patRefId, String referralId, String age, Item patient)
            throws ObjectStoreException {
        Item item = referrals.get(patRefId);
        if (item == null) {
            item = createItem("Referral");
            item.setAttribute("identifier", referralId);
//            if (!age.isEmpty()) {
                item.setAttributeIfNotNull("patientAge", age);
//            }
            if (patient != null) {
                item.setReference("patient", patient);
            }
            referrals.put(patRefId, item);
        }
        return item;
    }

    private void storePatients () throws ObjectStoreException {
        for (Item item : patients.values()) {
            Integer pid = store(item);
        }
    }

    private void storeReferrals () throws ObjectStoreException {
        for (Item item : referrals.values()) {
            Integer pid = store(item);
        }
    }


    private void processContact (Reader reader) throws Exception {
        // Read all lines into id pairs, track any ensembl ids or symbols that appear twice
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // Period,Patient ID,Referral ID,ReferralUrgency,ReferralSource,Referral accepted / rejected,
        // DischargeReason,ReferralDate,AssessmentDate,Date of first treatment contact,
        // DischargeDate,DischargeReason,Lifetime referrals to CAMHS,
        // AppointmentDate,AppointmentTypeDesc,AppointmentOutcomeDesc,TeamAtAppointment_Name
        //
        // e.g. (one line)
        // 2015-04-01-2019-03-31,1014003,2,Routine,Gp,Rejected,
        // Entered In Error,10/12/15,,,11/12/15,Entered In Error,1,
        // ,,,

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC CON " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String period = line[0];
            String patientId = line[1];
            String referralId = line[2];
            String urgency = line[3];
            String source = line[4];
            String outcome = line[5];
            String dischargeReason = line[6];
            String referralDate = line[7];
            String assessmentDate = line[8];
            String firstTreatmentDate = line[9];
            String dischargeDate = line[10];
            String discharegeReason = line[11];
            String cumulativeCAMHS = line[12];
            String appointmentDate = line[13];
            String appointmentOutcome = line[14];
            String appointmentTeam = line[15];

            // check if patient
            if (patients.get(patientId) == null) {
                LOG.warn("No patient found with identifier: " + patientId);
                continue;
            }
            // add attributes to referral
            String patRefId = patientId + "-" + referralId;  // to identify the referral
            if (referrals.get(patRefId) != null) {
                LOG.warn("Adding referral! " + patRefId);
                Item thisReferral = referrals.get(patRefId);
                if (!urgency.isEmpty()) {
                    thisReferral.setAttribute("urgency", urgency);
                }
                if (!source.isEmpty()) {
                    thisReferral.setAttribute("source", source);
                }
                if (!outcome.isEmpty()) {
                    thisReferral.setAttribute("outcome", outcome);
                }
                if (!dischargeReason.isEmpty()) {
                    thisReferral.setAttribute("dischargeReason", discharegeReason);
                }
//                if (!referralDate.isEmpty()) {
//                    thisReferral.setAttribute("date", referralDate);
//                }
//                if (!assessmentDate.isEmpty()) {
//                    thisReferral.setAttribute("assessment", assessmentDate);
//                }

            } else {
                LOG.warn("Plese create referral!");
            }
        }

        storeReferrals();
//        storePatients();
    }



    private void processDiagnosis(Reader reader) throws Exception {
//        public void process(Reader reader) throws Exception {


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
        LOG.info("PROC DIA " + Arrays.toString(header));
/*
        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String period = line[0];
            String patientId = line[1];
            String referralId = line[2];
            String age = line[3];
            String ethnicity = line[4];
            String gender = line[5];

            LOG.info("PAT: " + patientId);

            Item patient = createPatient(patientId, ethnicity, gender);

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
*/
    }




}

//    public void process(File dataDir) throws Exception {
//        List<File> files = readFilesInDir(dataDir);
//
//        for (File f : files) {
//            String fileName = f.getName();
//            if (fileName.endsWith("csv")) {
//                LOG.info("Reading file: " + fileName);
//                if (fileName.equalsIgnoreCase("campet.csv"))
//                processFile(new FileReader(f));
//            }
//        }
//    }

//    private List<File> readFilesInDir(File dir) {
//        List<File> files = new ArrayList<File>();
//        for (File file : dir.listFiles()) {
//            files.add(file);
//        }
//        return files;
//    }
