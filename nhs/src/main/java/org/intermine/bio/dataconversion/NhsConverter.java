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
    private static final String DATASET_TITLE = "Cambridge and Peterborough";
    private static final String DATA_SOURCE_NAME = "NHS";
    protected static final Logger LOG = Logger.getLogger(NhsConverter.class);

    private Map<String, Item> patients = new HashMap<>();   // patientId, patient
    private Map<String, Item> referrals = new HashMap<>();  // patRefId, referral
    private Map<String, Item> contacts = new HashMap<>();  // patRefId, referral
    private Map<String, Item> diagnostics = new HashMap<>();  // patRefId, diagnostic
    private Map<String, Item> dataSets = new HashMap<>();  // datasetName, dataSet

    private String dataSetRef = null; // to link patients to sites

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public NhsConverter(ItemWriter writer, Model model) {
        super(writer, model, null, DATASET_TITLE, null);
    }

    @Override
    public void process(Reader reader) throws Exception {
        File f = getCurrentFile();
        String fileName = getCurrentFile().getName();
        if (fileName.endsWith("csv")) {
            LOG.info("Reading file: " + fileName);
            createDataSet(DATASET_TITLE);   // using this loader only for cambridge dataset
            if (fileName.equalsIgnoreCase("campet.csv"))
                processDemographic(new FileReader(f));
            if (fileName.equalsIgnoreCase("campetPatLevDia.csv"))
                processDiagnosis(new FileReader(f));
            if (fileName.equalsIgnoreCase("campetPatLevCon.csv"))
                processContact(new FileReader(f));
        }

    }


    private void processDemographic(Reader reader) throws Exception {

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

            Item patient = createPatient(patientId, ethnicity, gender, DATASET_TITLE);
            Item referral = createReferral(patRefId, referralId, age, patient);
        }

        //storeReferrals();
        storePatients();

    }

    /**
     * create datasource and dataset
     *
     */
    private void createDataSet(String office)
            throws ObjectStoreException {
        Item dataSource = dataSets.get(office);
        if (dataSource == null) {
            dataSource = createItem("DataSource");
            dataSource.setAttribute("name", DATA_SOURCE_NAME);
            Item dataSet = createItem("DataSet");
            dataSet.setAttribute("name", office);
            dataSet.setAttribute("type", "accelerator");
            store(dataSource);
            dataSet.setReference("dataSource", dataSource.getIdentifier());
            store(dataSet);
            dataSetRef = dataSet.getIdentifier();
        }
            dataSets.put(office, dataSource);
        }


    private Item createPatient(String patientId, String ethnicity, String gender, String site)
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
            item.setAttributeIfNotNull("site", site);
            item.setReference("dataSet", dataSetRef);
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
            item.setAttributeIfNotNull("patientAge", age);
            if (patient != null) {
                item.setReference("patient", patient);
            }
            referrals.put(patRefId, item);
        }
        return item;
    }

    private Item createDiagnostic (String patientId, String patRefId, String assessmentDate, String observation)
            throws ObjectStoreException {
        Item referral = referrals.get(patRefId);
        Item patient = patients.get(patientId);
        Item dia = createItem("Diagnostic");
            dia.setAttributeIfNotNull("assessmentDate", assessmentDate);
            dia.setAttributeIfNotNull("observation", observation);
            if (patient != null) {
                dia.setReference("patient", patient);
            }
        if (referral != null) {
            dia.setReference("referral", referral);
        }
        return dia;
    }


    //
    // TODO: add methods to create items in different order (of loading files..)
    // for the moment we assume DEMO, CON, DIA
    //

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

    private void storeContacts () throws ObjectStoreException {
        for (Item item : contacts.values()) {
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
            //String dischargeReason = line[11]; repeated in csv
            String cumulativeCAMHS = line[12];
            String contactDate = line[13];
            String contactType = line[14];
            String contactOutcome = line[15];
            String contactTeam = line[16];

            // check if patient
            if (patients.get(patientId) == null) {
                LOG.warn("No patient found with identifier: " + patientId);
                continue;
            }
            // add attributes to referral
            String patRefId = patientId + "-" + referralId;  // to identify the referral
            if (referrals.get(patRefId) != null) {
                //LOG.info("Adding referral! " + patRefId);
                Item thisReferral = referrals.get(patRefId);
                thisReferral.setAttributeIfNotNull("urgency", urgency);
                thisReferral.setAttributeIfNotNull("source", source);
                thisReferral.setAttributeIfNotNull("outcome", outcome);
                thisReferral.setAttributeIfNotNull("dischargeReason", dischargeReason);
                thisReferral.setAttributeIfNotNull("referralDate", referralDate);
                thisReferral.setAttributeIfNotNull("assessmentDate", assessmentDate);
                thisReferral.setAttributeIfNotNull("firstTreatmentDate", firstTreatmentDate);
                thisReferral.setAttributeIfNotNull("dischargeDate", dischargeDate);
                thisReferral.setAttributeIfNotNull("cumulativeCAMHS", cumulativeCAMHS );
            } else {
                LOG.warn("Please check your CONTACT data: no referral " + referralId + " for patient "
                        + patientId +".");
            }
            Item contact = createContact(patientId, referralId, contactDate, contactType,
                    contactOutcome, contactTeam);

        }
//        storeReferrals();
//        storeContacts();
    }

    private Item createContact (String patientId, String referralId, String contactDate,
                                    String contactType, String contactOutcome, String contactTeam)
            throws ObjectStoreException {
        String patRefId = patientId + "-" + referralId;  // to identify the referral/contact
        Item item = contacts.get(patRefId);
        if (item == null) {
            item = createItem("Contact");
            item.setAttributeIfNotNull("contactDate", contactDate);
            item.setAttributeIfNotNull("contactType", contactType);
            item.setAttributeIfNotNull("contactOutcome", contactOutcome);
            item.setAttributeIfNotNull("team", contactTeam);
            Item patient = patients.get(patientId);
            if (patient != null) {
                item.setReference("patient", patient);
            }
            contacts.put(patRefId, item);
        }
        return item;
    }


    private void processDiagnosis(Reader reader) throws Exception {

        // Read all lines into id pairs, track any ensembl ids or symbols that appear twice
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // Period,Patient ID,Referral ID,ReferralTeamName,ICD10DiagnosisStartDate,DiagnosisEndDate,
        // ICD10Diagnosis,Assessment Date, + 51 fields that are flagged with yes/no/nothing in the sheet
        // see list below
        //
        // e.g. (first fields)
        // 2015-04-01-2019-03-31,1021295,2,CASUS,,,,,
        //
        // List of 51 'diagnosis'
        // Anxious Away From Caregivers, Anxious In Social Situations, Anxious Generally,
        // Compelled To Do Or Think Things, Panics, Avoids Going Out, Avoids Specific Things,
        // Repetitive Problematic Behaviours, Depression Low Mood, Self Harm, Extremes Of Mood,
        // Delusional Beliefs And Hallucinations, Drug And Alcohol Difficulties,
        // Difficulties Sitting Still Or Concentrating, Behavioural Difficulties, Poses Risk To Others,
        // Carer Management Of CYP Behaviour, Doesnt Get To Toilet In Time, Disturbed By Traumatic Event,
        // Eating Issues, Family Relationship Difficulties, Problems In Attachment To Parent Carer,
        // Peer Relationship Difficulties, Persistent Difficulties Managing Relationships With Others,
        // Does Not Speak, Gender Discomfort Issues, Unexplained Physical Symptoms,
        // Unexplained Developmental Difficulties, Self Care Issues, Adjustment To Health Issues,
        // Looked After Child, Young Carer Status, Learning Disability, Serious Physical Health Issues,
        // Pervasive Developmental Disorders, Neurological Issues, Current Protection Plan, Child In Need,
        // Refugee Or Asylum Seeker, Experience Of War Torture Or Trafficking, Experience Of Abuse Or Neglect,
        // Parental Health Issues, Contact With Youth Justice System, Living In Financial Difficulty, Home,
        // School Work Or Training, Community, ServiceEngagement, Attendance Difficulties, Attainment Difficulties
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC DIA " + Arrays.toString(header));
        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String period = line[0];
            String patientId = line[1];
            String referralId = line[2];
            String team = line[3];
            String startDate = line[4];
            String endDate = line[5];
            String ICD10diagnosis = line[6];
            String assessmentDate = line[7];

            // check if patient
            if (patients.get(patientId) == null) {
                LOG.warn("No patient found with identifier: " + patientId);
                continue;
            }
            // add attributes to referral
            String patRefId = patientId + "-" + referralId;  // to identify the referral
            if (referrals.get(patRefId) != null) {
                //LOG.info("Adding referral! " + patRefId);
                Item thisReferral = referrals.get(patRefId);
                thisReferral.setAttributeIfNotNull("referralTeam", team);
                thisReferral.setAttributeIfNotNull("diagnosisStartDate", startDate);
                thisReferral.setAttributeIfNotNull("diagnosisEndDate", endDate);
                thisReferral.setAttributeIfNotNull("ICD10diagnosis", ICD10diagnosis);
                thisReferral.setAttributeIfNotNull("assessmentDate", assessmentDate);
            } else {
                LOG.warn("Please check your CONTACT data: no referral " + referralId + " for patient "
                        + patientId +".");
                continue;
            }

            // create diagnostic (with date)
            // for each of the 51 diagnostics if yes add header to diagnostic (as observation)
            //
            for (int i=8; i<line.length; i++)
            {
                if (line[i].equalsIgnoreCase("yes"))
                {
                    LOG.debug("DDIIAA " + assessmentDate + ": " + header[i]);
                    store(createDiagnostic(patientId, patRefId, assessmentDate, header[i]));
                }
            }

        }

        storeReferrals();
        storeContacts();


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
