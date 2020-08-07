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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * @author
 */
public class PorConverter extends BioFileConverter {
    //
    //private static final String DATASET_TITLE = "Portsmouth";
    private static final String DATA_SOURCE_NAME = "NHS";
    private static final String SITE_CONTROL = "control";
    private static final String SITE_ITHRIVE = "accelerator";
    protected static final Logger LOG = Logger.getLogger(PorConverter.class);

    private Map<String, Item> patients = new HashMap<>();   // patientId, patient
    private Map<String, Item> referrals = new HashMap<>();  // patRefId, referral
    private Map<String, Item> contacts = new HashMap<>();  // patRefId, contact
    private Map<String, Item> dataSets = new HashMap<>();  // datasetName, dataSet
    //private Map<String, Item> diagnostics = new HashMap<>();  // patRefId, diagnostic
    private Map<String, String> ref2pat = new HashMap<>();  // referralId, patientId  (for worcester)

    private String dataSetRef = null; // to link patients to sites
    private String dataSet = null;  // to deal with differences in format
    private String siteType = null; // {ithrive, control}

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public PorConverter(ItemWriter writer, Model model) {
//        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE, null);
        super(writer, model, DATA_SOURCE_NAME, null, null);
    }

    /**
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
        File f = getCurrentFile();
        String fileName = getCurrentFile().getName();
        if (fileName.endsWith("csv")) {
            LOG.info("Reading file: " + fileName);

            // set datasource/dataset
            if (fileName.contains("Lewisham")) {
                siteType = SITE_CONTROL;
                dataSet = "Lewisham";
            }
            if (fileName.contains("NeCor")) {
                siteType = SITE_CONTROL;
                dataSet = "Ne-Cor";
            }
            if (fileName.contains("Portsmouth")) {
                dataSet = "Portsmouth";
                siteType = SITE_CONTROL;
            }
            if (fileName.contains("Southampton")) {
                dataSet = "Southampton";
                siteType = SITE_CONTROL;
            }
            if (fileName.contains("Stockport")) {
                dataSet = "Stockport";
                siteType = SITE_ITHRIVE;
            }
            if (fileName.contains("Sunderland")) {
                dataSet = "Sunderland";
                siteType = SITE_CONTROL;
            }
            if (fileName.contains("Worcester")) {
                dataSet = "Worcester";
                siteType = SITE_CONTROL;
            }
            createDataSet(dataSet, siteType);

            // process file
            if (fileName.contains("Patient")
                    || fileName.contains("Referral")  // ne-cor, worcester
                    || fileName.contains("Data"))     // sunderland
                processPatient(new FileReader(f));
            if (fileName.contains("Contact")
                    || fileName.contains("Activity")) // stockport
                processContact(new FileReader(f));
        }
    }


    /**
     * create datasource and dataset
     *
     */
    private void createDataSet(String site, String type)
            throws ObjectStoreException {
        Item dataSource = dataSets.get(site);
        if (dataSource == null) {
            dataSource = createItem("DataSource");
            dataSource.setAttribute("name", DATA_SOURCE_NAME);
            Item dataSet = createItem("DataSet");
            dataSet.setAttribute("name", site);
            dataSet.setAttribute("type", type);
            store(dataSource);
            dataSet.setReference("dataSource", dataSource.getIdentifier());
            store(dataSet);
            dataSetRef = dataSet.getIdentifier();
        }
            dataSets.put(site, dataSource);
        }


    private void processPatient(Reader reader) throws Exception {

        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // Patient ID,Referral ID ,Age at referral,Locality ,Ethnicity,Gender,Diagnosis,
        // Referral routine / urgent ,Referral source,Referral accepted / rejected,Referral date,
        // Triage date,Assessment date,Date of first treatment contact,Discharge date ,
        // Reason for discharge,Lifetime referrals to CAMHS,
        //
        // ----- the following are ignored (no data)
        // Date of contact / appointment 1,Appointment 1 routine / urgent ,
        // Appointment 1 contact type (face-to-face / non face-to-face),
        // "Attendance (attended, DNA, patient cancellation, service cancellation) for appointment 1",
        // Team at appointment 1,"Tier of team for appointment 1 (eg. Tier 2, 3, 4)",
        // Date of contact / appointment 2,Appointment 2 routine / urgent ,
        // Appointment 2 contact type (face-to-face / non face-to-face),
        // "Attendance (attended, DNA, patient cancellation, service cancellation) for appointment 2",
        // Team at appointment 2,"Tier of team for appointment 2 (eg. Tier 2, 3, 4)",
        // <-- please continue to generate columns for additional appointments
        //
        // e.g.
        // 26127609,31417853,11,,Z,F,,Routine,General Medical Practitioner,Accepted,03/11/15 11:24,
        // ,,,06/06/16 11:51,Discharged - Treatment completed,1,,,,,,,,,,,,,

        // for Ne-Cor:
        // [10] = period -> skip
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            // these can have different positions
//            String referralDate = null, triageDate = null, dischargeDate = null,
//                    dischargeReason = null, cumulativeCAMHS = null;

            String patientId = null;
            String referralId = null;
            String age = null;
            String locality = null;
            String ethnicity = null;
            String gender = null;
            String diagnosis = null;
            String urgency = null;
            String source = null;
            String outcome = null;
            String referralDate = null;
            String triageDate = null;
            String assessmentDate = null;
            String firstTreatmentDate = null;
            String dischargeDate = null;
            String dischargeReason = null;
            String cumulativeCAMHS = null;

            if (dataSet.contains("Lewisham")) {
                patientId = line[0];
                referralId = line[1];
                age = line[5];
                locality = line[13];
                ethnicity = line[2];
                gender = line[3];
                diagnosis = line[4];
                urgency = line[12];
                source = line[11];
                outcome = line[15];
                referralDate = line[6];
                //triageDate = line[11];
                assessmentDate = line[7];
                firstTreatmentDate = line[8];
                dischargeDate = line[10];
                dischargeReason = line[14];
                cumulativeCAMHS = line[18];

            } else if (dataSet.contains("Worcester")) {
                patientId = line[1];
                referralId = line[0];
                age = line[4];
                locality = line[17];
                ethnicity = line[3];
                gender = line[2];
                //diagnosis = line[6];
                urgency = line[7];
                source = line[6];
                //outcome = line[9];
                referralDate = line[5];
                //triageDate = line[11];
                //assessmentDate = line[12];
                firstTreatmentDate = line[8];
                dischargeDate = line[15];
                dischargeReason = line[18];
                //cumulativeCAMHS = line[16];

            } else {

                patientId = cleanIdentifier(line[0]);
                referralId = cleanIdentifier(line[1]);
                age = cleanIdentifier(line[2]);
                locality = line[3];
                ethnicity = line[4];
                gender = line[5];
                diagnosis = line[6];
                urgency = line[7];
                source = line[8];
                outcome = line[9];
                //referralDate = line[10];
                //triageDate = line[11];
                assessmentDate = line[12];
                firstTreatmentDate = line[13];
                //dischargeDate = line[14];
                //dischargeReason = line[15];
                //cumulativeCAMHS = line[16];

                if (dataSet.contains("Ne-Cor")) {
                    referralDate = line[11];
                    triageDate = line[12];
                    dischargeDate = line[15];
                } else if (dataSet.contains("Sunderland")) {
                    dischargeDate = line[13];
                    dischargeReason = line[14];
                    cumulativeCAMHS = line[15];
                } else {
                    referralDate = line[10];
                    triageDate = line[11];
                    dischargeDate = line[14];
                    dischargeReason = line[15];
                    cumulativeCAMHS = line[16];
                }
            }
            String patRefId = patientId + "-" + referralId;  // to identify the referral
            LOG.info("PAT: " + patRefId);
            ref2pat.put(referralId, patientId);

            Item patient = createPatient(patientId, ethnicity, gender);
            Item referral = createReferral(patRefId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS, patient);
        }

        storeReferrals();
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
            item.setReference("dataSet", dataSetRef);
            patients.put(patientId, item);
        }
        return item;
    }

    private Item createReferral(String patRefId, String referralId, String age, String locality,
            String diagnosis, String urgency, String source, String outcome, String referralDate,
            String triageDate, String assessmentDate, String firstTreatmentDate, String dischargeDate,
            String dischargeReason, String cumulativeCAMHS, Item patient)
            throws ObjectStoreException {
        Item item = referrals.get(patRefId);
        if (item == null) {
            item = createItem("Referral");
            item.setAttribute("identifier", referralId);
            item.setAttributeIfNotNull("patientAge", age);
            item.setAttributeIfNotNull("locality", locality);
            item.setAttributeIfNotNull("ICD10diagnosis", diagnosis);
            item.setAttributeIfNotNull("urgency", urgency);
            item.setAttributeIfNotNull("source", source);
            item.setAttributeIfNotNull("outcome", outcome);
            item.setAttributeIfNotNull("referralDate", referralDate);
            item.setAttributeIfNotNull("triageDate", triageDate);
            item.setAttributeIfNotNull("assessmentDate", assessmentDate);
            item.setAttributeIfNotNull("firstTreatmentDate", firstTreatmentDate);
            item.setAttributeIfNotNull("dischargeDate", dischargeDate);
            item.setAttributeIfNotNull("dischargeReason", dischargeReason);
            item.setAttributeIfNotNull("cumulativeCAMHS", cumulativeCAMHS);

            if (patient != null) {
                item.setReference("patient", patient);
            }
            referrals.put(patRefId, item);
        }
        return item;
    }

//    private Item createDiagnostic(String patientId, String patRefId, String assessmentDate, String observation)
//            throws ObjectStoreException {
//        Item referral = referrals.get(patRefId);
//        Item patient = patients.get(patientId);
//        Item dia = createItem("Diagnostic");
//        dia.setAttributeIfNotNull("assessmentDate", assessmentDate);
//        dia.setAttributeIfNotNull("observation", observation);
//        if (patient != null) {
//            dia.setReference("patient", patient);
//        }
//        if (referral != null) {
//            dia.setReference("referral", referral);
//        }
//        return dia;
//    }


    //
    // TODO: add methods to create items in different order (of loading files..)
    // for the moment we assume DEMO, CON, DIA
    //

    private void storePatients() throws ObjectStoreException {
        for (Item item : patients.values()) {
            Integer pid = store(item);
        }
    }

    private void storeReferrals() throws ObjectStoreException {
        for (Item item : referrals.values()) {
            Integer pid = store(item);
        }
    }

    private void storeContacts() throws ObjectStoreException {
        for (Item item : contacts.values()) {
            Integer pid = store(item);
        }
    }

    private void storeAll() throws ObjectStoreException {
        storePatients();
        storeReferrals();
        storeContacts();
    }

    private void processContact(Reader reader) throws Exception {
         Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // Patient ID,Referral ID ,Appointment ID,Contact Number,Date of contact ,
        // Appointment routine / urgent ,Appointment contact type , Attendance ,Team ,Tier of team
        //
        // e.g. (one line)
        // 20821007,22122956,2066926618,1,11/04/16 15:00,,f2f,Finished,,
        //
        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC CON " + Arrays.toString(header));

        // define headers
        String patientId = null, referralId = null, contactId = null, contactDate = null,
                ordinal = null, urgency = null,
                contactType = null, attendance = null, outcome = null, team = null, tier = null;

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            if (dataSet.contains("Worcester")) {
                referralId = line[0];
                contactId = cleanIdentifier(line[2]);
                contactDate = line[3];
                ordinal = line[4];
                contactType = line[5];
                attendance = line[6];
                team = line[7];

            } else {
                patientId = cleanIdentifier(line[0]);
                referralId = cleanIdentifier(line[1]);

                if (dataSet.contains("Na-Cor")) {
                    contactDate = line[3];
                    contactType = line[4];
                    team = line[5];
                } else if (dataSet.contains("Stockport")) {
                    contactId = cleanIdentifier(line[2]);
                    contactDate = line[3];
                    urgency = line[4];
                    contactType = line[6];
                    attendance = line[5];
                    team = line[7];
                } else {
                    try {
                        contactId = line[2];
                        ordinal = line[3];
                        contactDate = line[4];
                        urgency = line[5];
                        contactType = line[6];
                        attendance = line[7];
                        team = line[8];
                        tier = line[9];
                    } catch (ArrayIndexOutOfBoundsException exception) {
                        // the sheets have different lenghts
                        continue;
                    }

                }
                // check if patient
                if (patients.get(patientId) == null) {
                    LOG.warn("No patient found with identifier: " + patientId);
                    continue;
                }
            }

                Item contact = createContact(patientId, referralId, contactId, ordinal,
                        contactDate, urgency, contactType, attendance, outcome, team, tier);
            }

        //storeReferrals();
        storeContacts();
    }

    private Item createContact(String patientId, String referralId, String contactId,
                                   String ordinal, String contactDate, String urgency,
                                   String contactType, String attendance, String outcome, String team, String tier)
            throws ObjectStoreException {

        if (patientId == null) {
            patientId = ref2pat.get(referralId);
        }
        String patRefId = patientId + "-" + referralId;  // to identify the referral/contact
        LOG.info("PATREF CON " + patRefId);


        Item item = contacts.get(patRefId);
        if (item == null) {
            item = createItem("Contact");
            item.setAttributeIfNotNull("identifier", contactId);
            item.setAttributeIfNotNull("ordinal", ordinal);
            item.setAttributeIfNotNull("contactDate", contactDate);
            item.setAttributeIfNotNull("urgency", urgency);
            item.setAttributeIfNotNull("contactType", contactType);
            item.setAttributeIfNotNull("attendance", attendance);
            item.setAttributeIfNotNull("contactOutcome", outcome);
            item.setAttributeIfNotNull("team", team);
            item.setAttributeIfNotNull("teamTier", tier);
            Item patient = patients.get(patientId);
            Item referral = referrals.get(patRefId);
            if (patient != null) {
                item.setReference("patient", patient);
            }
            if (referral != null) {
                item.setReference("referral", referral);
            }

            contacts.put(patRefId, item);
        }
        return item;
    }

    private String cleanIdentifier(String identifier) {
        // needed for stockport, e.g.:
        // patientId = RT2550527
        // referralId = 3_155204910
        // contactId = 5_C_2480976  (activity Id)
        //
        // worcester:
        // contactId = 1150471DA
        //
        // TODO: check if rather do a process just for stockport
        //   no: cases in the various processes
        // TODO: merge the last 3 cases

        String cleanId = identifier;
        if (identifier.startsWith("RT")) {
//            LOG.info("IIDD-RT " + cleanId + "->" + identifier.replace("RT", ""));
           return identifier.replace("RT", "");
        }
        if (identifier.contains("_")) {
            String[] splitted = identifier.split("_");
//            LOG.info("IIDD-US " + cleanId + "->" + splitted[splitted.length -1]);
            return splitted[splitted.length -1];
        }
//        LOG.info("IIDD-nil " + identifier);
        if (identifier.equalsIgnoreCase("NULL")) {
            LOG.info("IIDD-NULL ");
            return null;
        }
        if (identifier.endsWith("DA")) {
//            LOG.info("IIDD-RT " + cleanId + "->" + identifier.replace("RT", ""));
            return identifier.replace("DA", "");
        }
        if (identifier.endsWith("CA")) {
//            LOG.info("IIDD-RT " + cleanId + "->" + identifier.replace("RT", ""));
            return identifier.replace("CA", "");
        }
        if (identifier.endsWith("GA")) {
//            LOG.info("IIDD-RT " + cleanId + "->" + identifier.replace("RT", ""));
            return identifier.replace("GA", "");
        }


        return identifier;
    }


/*
    private void processDiagnosis(Reader reader) throws Exception {

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
                        + patientId + ".");
                continue;
            }

            // create diagnostic (with date)
            // for each of the 51 diagnostics if yes add header to diagnostic (as observation)
            //
            for (int i = 8; i < line.length; i++) {
                if (line[i].equalsIgnoreCase("yes")) {
                    LOG.info("DDIIAA " + assessmentDate + ": " + header[i]);
                    store(createDiagnostic(patientId, patRefId, assessmentDate, header[i]));
                }
            }

        }

        storeReferrals();
        storeContacts();


    }
*/
}