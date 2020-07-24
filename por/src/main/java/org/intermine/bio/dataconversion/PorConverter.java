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
    protected static final Logger LOG = Logger.getLogger(PorConverter.class);

    private Map<String, Item> patients = new HashMap<>();   // patientId, patient
    private Map<String, Item> referrals = new HashMap<>();  // patRefId, referral
    private Map<String, Item> appointments = new HashMap<>();  // patRefId, appointment
    private Map<String, Item> dataSets = new HashMap<>();  // datasetName, dataSet
    //private Map<String, Item> diagnostics = new HashMap<>();  // patRefId, diagnostic

    private String dataSetRef = null; // to link patients to sites
    private String dataSet = null; // to deal with differences in format

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
            if (fileName.contains("Southampton"))
                dataSet = "Southampton";
            if (fileName.contains("Portsmouth"))
                dataSet = "Portsmouth";
            if (fileName.contains("NeCor"))
                dataSet = "Ne-Cor";
            createDataSet(dataSet);

            // process file
            if (fileName.contains("Patient") || fileName.contains("Referral")) // referral for ne-cor
                processPatient(new FileReader(f));
            if (fileName.contains("Contact"))
                processContact(new FileReader(f));
        }
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
            store(dataSource);
            dataSet.setReference("dataSource", dataSource.getIdentifier());
            store(dataSet);
            dataSetRef = dataSet.getIdentifier();
        }
            dataSets.put(office, dataSource);
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
            String referralDate, triageDate, dischargeDate;

            String patientId = line[0];
            String referralId = line[1];
            String age = line[2];
            String locality = line[3];
            String ethnicity = line[4];
            String gender = line[5];
            String diagnosis = line[6];
            String urgency = line[7];
            String source = line[8];
            String outcome = line[9];
            //String referralDate = line[10];
            //String triageDate = line[11];
            String assessmentDate = line[12];
            String firstTreatmentDate = line[13];
            //String dischargeDate = line[14];
            String dischargeReason = line[15];
            String cumulativeCAMHS = line[16];

            if (dataSet.contains("Ne-Cor")) {
                referralDate = line[11];
                triageDate = line[12];
                dischargeDate = line[15];
            } else {
                referralDate = line[10];
                triageDate = line[11];
                dischargeDate = line[14];
            }

            String patRefId = patientId + "-" + referralId;  // to identify the referral
            //LOG.info("PAT: " + patRefId);

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

    private void storeAppointments() throws ObjectStoreException {
        for (Item item : appointments.values()) {
            Integer pid = store(item);
        }
    }

    private void storeAll() throws ObjectStoreException {
        storePatients();
        storeReferrals();
        storeAppointments();
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
        String appointmentId = null, ordinal = null, appointmentDate = null, urgency = null,
                appointmentType = null, attendance = null, team = null, tier = null;

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String patientId = line[0];
            String referralId = line[1];

            if (dataSet.contains("Na-Cor")) {
                appointmentDate = line[3];
                appointmentType = line[4];
                team = line[5];
            } else {
                try {
                appointmentId = line[2];
                ordinal = line[3];
                appointmentDate = line[4];
                urgency = line[5];
                appointmentType = line[6];
                attendance = line[7];
                team = line[8];
                tier = line[9];
                }  catch (ArrayIndexOutOfBoundsException exception) {
                // the sheets have different lenghts
                continue;
                }

            }
            // check if patient
            if (patients.get(patientId) == null) {
                LOG.warn("No patient found with identifier: " + patientId);
                continue;
            }

            Item appointment = createAppointment(patientId, referralId, appointmentId, ordinal,
                    appointmentDate, urgency, appointmentType, attendance, team, tier);
        }
        //storeReferrals();
        storeAppointments();
    }

    private Item createAppointment(String patientId, String referralId, String appointmentId,
                                   String ordinal, String appointmentDate, String urgency,
                                   String appointmentType, String attendance, String team, String tier)
            throws ObjectStoreException {
        String patRefId = patientId + "-" + referralId;  // to identify the referral/appointment
        Item item = appointments.get(patRefId);
        if (item == null) {
            item = createItem("Appointment");
            item.setAttributeIfNotNull("identifier", appointmentId);
            item.setAttributeIfNotNull("ordinal", ordinal);
            item.setAttributeIfNotNull("appointmentDate", appointmentDate);
            item.setAttributeIfNotNull("urgency", urgency);
            item.setAttributeIfNotNull("contactType", appointmentType);
            item.setAttributeIfNotNull("contactOutcome", attendance);
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

            appointments.put(patRefId, item);
        }
        return item;
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
        storeAppointments();


    }
*/
}