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
    private static final String DATA_SOURCE_NAME = "NHS";
    private static final String SITE_CONTROL = "control";
    private static final String SITE_ITHRIVE = "accelerator";
    private static final String ADD_CLASS = "AdditionalData";
    private static final String CCD_CLASS = "CumulativeContactData";
    protected static final Logger LOG = Logger.getLogger(PorConverter.class);

    private Map<String, Item> patients = new HashMap<>();   // patientId, patient
    private Map<String, Item> referrals = new HashMap<>();  // patRefId, referral
    private Map<String, Item> contacts = new HashMap<>();  // patRefId, contact
    private Map<String, Item> dataSets = new HashMap<>();  // datasetName, dataSet
    //private Map<String, Item> diagnostics = new HashMap<>();  // patRefId, diagnostic
    private Map<String, String> ref2pat = new HashMap<>();  // referralId, patientId  (for worcester)

//    private Map<String, String> lineRedux = new HashMap<>();  // designed term, value read
//    private String[] patientStage = {
//    "patientId",
//    "referralId",
//    "age",
//    "locality",
//    "ethnicity",
//    "gender",
//    "diagnosis",
//    "urgency",
//    "source",
//    "outcome",
//    "referralDate",
//    "triageDate",
//    "assessmentDate",
//    "firstTreatmentDate",
//    "dischargeDate",
//    "dischargeReason",
//    "cumulativeCAMHS"
//};


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
        //
        // TODO: sort out the ordering of files (can be avoided? stardardise naming?)
        //     : merge with the 3 files cases (e.g. cambridge)

        File f = getCurrentFile();
        String fileName = getCurrentFile().getName();
        if (fileName.endsWith("csv")) {
            LOG.info("Reading file: " + fileName);

            // set datasource/dataset
            setDataset(fileName);

            // process file
            if (fileName.contains("Warrington")) {
                processWarrington(new FileReader(f));
            } else {
                if (fileName.contains("Patient")
                        || fileName.contains("Referral")  // ne-cor, worcester
                        || fileName.contains("Data"))     // sunderland
                    processPatient(new FileReader(f));
                if (fileName.contains("Contact")
                        //  || fileName.contains("Outcome")   // lewisham
                        || fileName.contains("Activity")) // stockport
                    processContact(new FileReader(f));
                if (fileName.contains("Outcome"))   // waltham
                    processDiagnosis(new FileReader(f));
                // these have only one file
                if (fileName.contains("Bexley"))
                    processBexley(new FileReader(f));
                if (fileName.contains("Camden"))
                    processCamden(new FileReader(f));
                if (fileName.contains("Luton"))
                    processLuton(new FileReader(f));
                if (fileName.contains("Norfolk"))
                    processNorfolk(new FileReader(f));
                if (fileName.contains("Stoke"))
                    processStoke(new FileReader(f));
            }
        }
    }

    private void processPatient(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption: too many to report.. below the original one for cambridge
        //
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

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            // (issue with waltham)
            // TODO? improve
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // TODO: something better..
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
            } else if (dataSet.contains("Waltham")) {
                patientId = cleanIdentifier(line[0]);
                referralId = cleanIdentifier(line[1]);
                age = cleanIdentifier(line[2]);
                locality = line[3];
                ethnicity = line[4];
                gender = line[5];
                diagnosis = cleanIdentifier(line[6]);
                urgency = line[7];
                source = line[8];
                outcome = line[9];
                referralDate = line[10];
                triageDate = cleanIdentifier(line[11]);
                assessmentDate = line[12];
                firstTreatmentDate = line[13];
                dischargeDate = line[14];
                dischargeReason = line[15];
                cumulativeCAMHS = cleanIdentifier(line[16]);
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
//            String patRefId = patientId + "-" + referralId;  // to identify the referral
//            LOG.info("PAT: " + patRefId);
            ref2pat.put(referralId, patientId);

            Item patient = createPatient(patientId, ethnicity, gender);
            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            if (dataSet.contains("Waltham")) {
                // waltham has contact info in the same sheet
                String contactDate = cleanIdentifier(line[17]);
                String contactUrgency = cleanIdentifier(line[18]);
                String contactType = line[19];
                String attendance = cleanIdentifier(line[20]);
                String team = cleanIdentifier(line[21]);
                String tier = cleanIdentifier(line[22]);
                String ordinal = line[23];
                String contactId = null;

                Item contact = createContact(patientId, referralId, contactId, ordinal,
                        contactDate, contactUrgency, contactType, attendance, outcome, team, tier);
            }
        }

        if (dataSet.contains("Waltham")) {
            // store after diagnostic
            //storeContacts();
            return;
        }

        storeReferrals();
        storePatients();

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

            } else if (dataSet.contains("Lewisham")) {
                patientId = line[0];
                referralId = line[1];
                contactId = line[2];
                contactDate = line[3];
                outcome = line[4];
                contactType = line[5];
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
//                if (patients.get(patientId) == null) {
//                    LOG.warn("No patient found with identifier: " + patientId);
//                    continue;
//                }
            }
            // check if patient
            if (patients.get(patientId) == null) {
                LOG.warn("No patient found with identifier: " + patientId);
                continue;
            }

            Item contact = createContact(patientId, referralId, contactId, ordinal,
                    contactDate, urgency, contactType, attendance, outcome, team, tier);
        }

        //storeReferrals();
        storeContacts();
    }

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
        // used only by waltham for now (TODO: maybe lewisham too?)
        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();

            String period = line[0];
            String patientId = cleanIdentifier(line[1]);
            String referralId = cleanIdentifier(line[3]);
            String team = line[4];
//            String startDate = line[4];
//            String endDate = line[5];
//            String ICD10diagnosis = line[6];
//            String assessmentDate = line[7];

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
//                thisReferral.setAttributeIfNotNull("diagnosisStartDate", startDate);
//                thisReferral.setAttributeIfNotNull("diagnosisEndDate", endDate);
//                thisReferral.setAttributeIfNotNull("ICD10diagnosis", ICD10diagnosis);
//                thisReferral.setAttributeIfNotNull("assessmentDate", assessmentDate);
            } else {
                LOG.warn("Please check your CONTACT data: no referral " + referralId + " for patient "
                        + patientId + ".");
                continue;
            }

            // create diagnostic
            // for waltham outcome file, with a format
            // [Financialyr,LocalPatientID,Age,ReferralID,RefTeamDescription,Locality,]
            // initial cgasScore,last cgasScore,initial honosca,last honosca
            // e.g.
            // 2018/2019,1038393RiO,16,1038393MHSRef11,T3-RB- Tier 3 CAMHS,NHS WALTHAM FOREST CCG,,,210332003323400,
            for (int i = 6; i < 9; i++) {
                //LOG.info("DDIIAA: " + header[i]);
                store(createDiagnostic(patientId, referralId, null, header[i], line[i + 1]));
                //}
            }

        }

        storePatients();
        storeReferrals();
        storeContacts();

    }

    private void processWarrington(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption: too many to report.. below the original one for cambridge
        //
        // Patient ID,Referral ID ,Age at referral,Locality ,Ethnicity,Gender,Diagnosis,
        // Referral routine / urgent ,Referral source,Referral accepted / rejected,Referral date,
        // Triage date,Assessment date,Date of first treatment contact,Discharge date ,
        // Reason for discharge,Lifetime referrals to CAMHS,
        //
        // e.g.
        // 26127609,31417853,11,,Z,F,,Routine,General Medical Practitioner,Accepted,03/11/15 11:24,
        // ,,,06/06/16 11:51,Discharged - Treatment completed,1,,,,,,,,,,,,,

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            // (issue with waltham) TODO? improve
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // TODO: something better..
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
            String patRefId = null;

            if (getCurrentFile().getName().contains("Patient")) {
                patientId = line[1];
                ethnicity = line[2];
                gender = line[3];

                Item patient = createPatient(patientId, ethnicity, gender);

                // create patient additional data
                // Patient_DisabilityFlag,DiagnosisCode_Primary,
                // Length of treatment from assessment to discharge,Was this patient signposted after discharge?
                // e.g.
                // No,N/A,5,N
                for (int i = 4; i < 8; i++) {
                    store(createAdditionalData(patientId, null, ADD_CLASS, header[i], line[i]));
                }
            } else { // the contact file
                patientId = line[1];
                referralId = line[3];
                age = line[2];
                outcome = line[4];

//                ref2pat.put(referralId, patientId);

                Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                        source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                        dischargeDate, dischargeReason, cumulativeCAMHS);

                // create cumulative contact data
                // [FinYear2,Patient_Identif,Age,rerefs,CAMHS referral outcome,] in referral
                // Routine waiting times for first appt,Emergency waiting times for first appt,
                // Urgent waiting times for first appt,Was this patient seen for assessment?,
                // F2FAttends,NonF2FAttends,DNAs,service/NHSCacellation,PatientCacellation,
                // Was the patient discharged?
                // e.g.
                // 2015/16,7,16,1,Discharged treatment completed,N/A,N/A,N/A,N,0,0,0,0,0,Y
                //
                for (int i = 5; i < 15; i++) {
                    store(createAdditionalData(patientId, referralId, CCD_CLASS , header[i], line[i]));
                }
            }
        }
        if (getCurrentFile().getName().contains("Patient"))
            storePatients();
        else storeReferrals();
    }

    private void processBexley(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // PASID,ReferralNumber,Ethnicity,Gender,ReferralUrgencyCode,ReferralUrgencyDescription,ReferralID,
        // TeamReferredTo,TeamReferredToDescription,DaysReferralToDischarge,ReferralReceivedDate,
        // ReferralDischargedDate,FirstApptToRef,RefToFirstApptWeeks,Borough,face2faceappt,Nonface2faceappt,
        // DNA,ProviderCancellations,ICD10CodingScheme,ICD10DiagnosisCode,ICD10DiagnosisDescription,
        // ICD10DiagnosisStartDate,ICD10DiagnosisEndDate,
        // NCDSCodingScheme,NCDSDiagnosisCode,NCDSDiagnosisDescription,NCDSDiagnosisStartDate,NCDSDiagnosisEndDate,
        // PreviousReferralNumber,PreviousTeamReferredTo,PreviousTeamReferredToDescription,
        // PreviousReferralReceivedDate,PreviousReferralDischargedDate
        //
        // e.g.
        // 1027461,4,White - British,Male,NU,Non Urgent,457487,
        // BECLAC,Bexley CAMHS LAC,472,01/05/15,15/08/16,20/05/15,3,Bexley,8,NULL,1,2,NULL,NULL,NULL,NULL,NULL,
        // NCDS,2,"Emotional Disorders, includes OCD, PTSD",27/10/15,NULL,3,
        // BECLAC,Bexley CAMHS LAC,04/12/08,01/02/10
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // only one file..
            String patientId = line[0];
            String referralId = line[6];
            String age = null;
            String locality = line[14];
            String ethnicity = line[2];
            String gender = line[3];
            String diagnosis = line[20];
            String urgency = line[5];
            String source = null;
            String outcome = null;
            String referralDate = line[10];
            String triageDate = null;
            String assessmentDate = null;
            String firstTreatmentDate = line[12];
            String dischargeDate = line[11];
            String dischargeReason = null;
            String cumulativeCAMHS = null;

            Item patient = createPatient(patientId, ethnicity, gender);

            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            // create patient additional data
            int[] looper = {4,7,8,29,30,31,32,33};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, ADD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // create cumulative contact data
            looper = new int[]{9,13,15,16,17,18};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, CCD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // create diagnostics
            int[] looperD = {21,24,25,26,27,28};
            for (int i = 0; i < looperD.length; i++) {
                store(createDiagnostic(patientId, referralId, null, header[looperD[i]], line[looperD[i]]));
            }
        }
        storePatients();
        storeReferrals();
    }

    private void processCamden(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // ReferralActivityID,PatientID,AgeAtReferral,Ethnicity,Gender,PrimaryDiagnosisList,ReferralPriority,
        // ReferralSource,AcceptedOrRejected,ReferralDate,ReferralReceivedDate,TriageDate,SecondAttendedAppointment,
        // AssessmentStart,AssessmentEnd,FirstAttendedAssessmentAppointment,ThirdAttendedAppointment,TreatmentStart,
        // TreatmentEnd,FirstAttendedTreatmentAppointment,DischargeDate,DischargeReason,ReferralActivityByPatient,
        // LifetimeReferralsToCAMHS,
        // Appointment1Date,Appointment1Team,Appointment1ContactType,Appointment1Attendance
        // [....] up to line Appointment615...
        //
        // e.g.
        //
        // 62
        //69,6079,16.1,White - British,Female,,Non Urgent,Other clinical specialty,Accepted,26/09/14,26/09/14,
        // NA,22/01/15,26/09/14,21/01/15,26/09/14,25/02/15,22/01/15,27/01/16,22/01/15,27/01/16,PATIENT non-attendance,
        // 2,2,26/09/14,NORTH Service,F2F,Attended,22/01/15,NORTH Service,F2F,Attended,25/02/15,NORTH Service,F2F,
        // Carer Attended,17/03/15,NORTH Service,F2F,Attended,19/06/15,NORTH Service,F2F,Attended,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        // ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        //LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // only one file..

            String patientId = line[1];
            String referralId = line[0];
            String age = null;
            String locality = null;
            String ethnicity = line[3];
            String gender = line[4];
            String diagnosis = line[5];
            String urgency = line[6];
            String source = line[7];
            String outcome = line[8];
            String referralDate = line[9];
            String triageDate = line[11];
            String assessmentDate = null;
            String firstTreatmentDate = line[19];
            String dischargeDate = line[20];
            String dischargeReason = line[21];
            String cumulativeCAMHS = line[23];
            // they have ages like 4.2.
            // rounding to the integer.
            if (line[2].contains("."))
                age = line[2].substring(0, line[2].indexOf('.'));
            else
                age =line[2];

            Item patient = createPatient(patientId, ethnicity, gender);

            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            // create patient additional data
            int[] looper = {12,13,14,15,16,17,18,22};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, ADD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // this should deal with the potential 615 contacts recorded on each line
            // (4 attributes for each contact)
            for (int j = 24; j < 24*615; j+=3) {
                if (line[j].isEmpty()) break; //stop if you find no value
                String contactDate = line[j];
                String team = line[j+1];
                String contactType = line[j+2];
                String attendance = line[j+3];

                Item contact = createContact(patientId, referralId, null, null,
                        contactDate, null, contactType, attendance, outcome, team, null);
            }

//            // create cumulative contact data
//            looper = new int[]{9,13,15,16,17,18};
//            for (int i = 0; i < looper.length; i++) {
//                store(createAdditionalData(patientId, referralId, CCD_CLASS, header[looper[i]], line[looper[i]]));
//            }
//
//            // create diagnostics
//            int[] looperD = {21,24,25,26,27,28};
//            for (int i = 0; i < looperD.length; i++) {
//                store(createDiagnostic(patientId, referralId, null, header[looperD[i]], line[looperD[i]]));
//            }
        }
        storePatients();
        storeReferrals();
        storeContacts();
    }

    private void processStoke(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // PatientID,ReferralID,AgeAtReferral,Locality,Ethnicity,Gender,Diagnosis,PriorityType,SourceOfReferral,
        // ReferralAccepted/Rejected,ReferralDate,TriageDate,AssessmentDate,FirstTreatmentContact,DischargeDate,
        // ReasonForDischarge,LifetimeReferrals,
        // DateOfContact1,AppointmentRoutineUrgent1,ContactType1,Attendance1,TeamAtAppointment1,TierOfTeam1,
        // [...] up Contact 50...
        //
        // e.g.
        // 650000217082,650001309292,13,Ipstones,A,F,NotAvailable,Routine,General Medical Practitioner,Rejected,
        // 17/12/18,NotAvailable,,,18/12/18,Inappropriate Referral,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        // NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NUL
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        //LOG.info("PROC PAT " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // only one file..

            String patientId = line[1];
            String referralId = line[0];
            String age = null;
            String locality = null;
            String ethnicity = line[3];
            String gender = line[4];
            String diagnosis = line[5];
            String urgency = line[6];
            String source = line[7];
            String outcome = line[8];
            String referralDate = line[9];
            String triageDate = line[11];
            String assessmentDate = null;
            String firstTreatmentDate = line[19];
            String dischargeDate = line[20];
            String dischargeReason = line[21];
            String cumulativeCAMHS = line[23];
            // they have ages like 4.2.
            // rounding to the integer.
            if (line[2].contains("."))
                age = line[2].substring(0, line[2].indexOf('.'));
            else
                age =line[2];

            Item patient = createPatient(patientId, ethnicity, gender);

            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            // create patient additional data
            int[] looper = {12,13,14,15,16,17,18,22};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, ADD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // this should deal with the potential 615 contacts recorded on each line
            // (4 attributes for each contact)
            for (int j = 24; j < 24*615; j+=3) {
                if (line[j].isEmpty()) break; //stop if you find no value
                String contactDate = line[j];
                String team = line[j+1];
                String contactType = line[j+2];
                String attendance = line[j+3];

                Item contact = createContact(patientId, referralId, null, null,
                        contactDate, null, contactType, attendance, outcome, team, null);
            }

//            // create cumulative contact data
//            looper = new int[]{9,13,15,16,17,18};
//            for (int i = 0; i < looper.length; i++) {
//                store(createAdditionalData(patientId, referralId, CCD_CLASS, header[looper[i]], line[looper[i]]));
//            }
//
//            // create diagnostics
//            int[] looperD = {21,24,25,26,27,28};
//            for (int i = 0; i < looperD.length; i++) {
//                store(createDiagnostic(patientId, referralId, null, header[looperD[i]], line[looperD[i]]));
//            }
        }
        storePatients();
        storeReferrals();
        storeContacts();
    }


    private void processLuton(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // ID,Referral_id,Ethnicity,Gender,Diagnosis,ReferralUrgency,Referral Source,Referral accepted/rejected,
        // Referral Date,Assessment Date,Date of first treatment contact,Discharge Date,DischargeReason,
        // Lifetime referrals to CAMHS,Date of contact/appointment,contact type,Attendance,Team at appointment,Service
        //
        // e.g.
        // TH1062894,4,Asian or Asian British - Bangladeshi              ,f,,Routine,
        // Local Authority Social Services,Accepted,03/02/16,05/02/16 14:30,05/02/16 14:30,28/03/17,
        // Discharged on professional advice,2,05/02/16 14:30,f2f,Attended,TH CAMHS LBTH,TH
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC Luton " + Arrays.toString(header));

        while (lineIter.hasNext()) {
            String[] line = (String[]) lineIter.next();
            // check if empty
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // only one file..
            String patientId = cleanIdentifier(line[0]); //TODO: put prefix in source LT, TH
            String referralId = cleanIdentifier(line[1]);
            String age = null;
            String locality = line[14];
            String ethnicity = line[2];
            String gender = line[3];
            String diagnosis = line[4];
            String urgency = line[5];
            String source = line[6];
            String outcome = line[7];
            String referralDate = cleanIdentifier(line[8]);
            String triageDate = null;
            String assessmentDate = cleanIdentifier(line[9]);       //NULL
            String firstTreatmentDate = cleanIdentifier(line[10]);  //NULL
            String dischargeDate = cleanIdentifier(line[11]);       //NULL
            String dischargeReason = cleanIdentifier(line[12]);     //NULL
            String cumulativeCAMHS = cleanIdentifier(line[13]);
            String contactDate = cleanIdentifier(line[14]);         //NULL
            String contactType = line[15];
            String attendance = cleanIdentifier(line[16]);
            String team = line[17];
            String tier = line[18];

            Item patient = createPatient(patientId, ethnicity, gender);

            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            Item contact = createContact(patientId, referralId, null, null,
                    contactDate, urgency, contactType, attendance, outcome, team, tier);

            /*
            // create patient additional data
            int[] looper = {4,7,8,29,30,31,32,33};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, ADD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // create cumulative contact data
            looper = new int[]{9,13,15,16,17,18};
            for (int i = 0; i < looper.length; i++) {
                store(createAdditionalData(patientId, referralId, CCD_CLASS, header[looper[i]], line[looper[i]]));
            }

            // create diagnostics
            int[] looperD = {21,24,25,26,27,28};
            for (int i = 0; i < looperD.length; i++) {
                store(createDiagnostic(patientId, referralId, null, header[looperD[i]], line[looperD[i]]));
            }

             */
        }
        storePatients();
        storeReferrals();
        storeContacts();
    }

    private void processNorfolk(Reader reader) throws Exception {
        Iterator lineIter = FormattedTextParser.parseCsvDelimitedReader(reader);

        // format assumption:
        // PatientID,ReferralID,Ethnicity,Gender,Diagnosis,ClinicalOutcomeMeasure,AgeAtReferral,Locality,
        // ReferralDate,TriageDate,AssessmentDate,DateOfFirstTreatment,DischargeDate,ReferralSource,
        // ReferralUrgent/Routine,TeamAtContact,Tier,ReasonForDischarge,CAMHSReferralOutcome,LifetimeReferralsToCAMHS,
        // DateOfEachAppointment,AttendanceAtEachAppointment,ContactTypeForEachAppointment,
        // Urgent/routineForEachAppointmen
        //
        // e.g.
        // RMY095954,167977,White - British,Female,,,14,NHS NORWICH CCG,
        // 10/02/11,,27/02/13,27/02/13,17/02/16,Local Authority Social Services,
        // ,***Northern  CAMHS,3,CC_RFDISCHOTHEPROV,,2,27/10/11,Attended,F2F,
        //

        // parse header in case
        String[] header = (String[]) lineIter.next();
        LOG.info("PROC Norfolk " + Arrays.toString(header));
        int lineCount = 1; // in excel line numbers start at 1 including header
        while (lineIter.hasNext()) {
            lineCount ++; // line number in the original file
            String[] line = (String[]) lineIter.next();
            // check if empty
            if (line[0].equals(null) || line[0].equals(""))
                continue;

            // only one file..
            String patientId = cleanIdentifier(line[0]);
            if (patientId.contains("/"))
            {
                LOG.warn("Unrecognised patient ID " + line[0] + " in line " + lineCount);
            continue;
            }
            String referralId = line[1];
            if (referralId.contains("E+"))
            {
                LOG.warn("Unrecognised referral ID " + line[1] + " in line " + lineCount +
                        " for patient " + line[0]);
                continue;
            }
            String age = line[6];
            String locality = line[7];
            String ethnicity = line[2];
            String gender = line[3];
            String diagnosis = line[4];
            String urgency = line[14];
            String source = line[13];
            String outcome = line[5];
            String referralDate = line[8];
            String triageDate = line[9];
            String assessmentDate = line[10];
            String firstTreatmentDate = line[11];
            String dischargeDate = line[12];
            String dischargeReason = line[17];
            String cumulativeCAMHS = line[19];
            String contactDate = line[20];
            String contactType = line[22];
            String attendance = line[21];
            String team = line[15];
            String tier = line[16];
            String contactUrgency = line[23];

            Item patient = createPatient(patientId, ethnicity, gender);

            Item referral = createReferral(patientId, referralId, age, locality, diagnosis, urgency,
                    source, outcome, referralDate, triageDate, assessmentDate, firstTreatmentDate,
                    dischargeDate, dischargeReason, cumulativeCAMHS);

            Item contact = createContact(patientId, referralId, null, null,
                    contactDate, contactUrgency, contactType, attendance, outcome, team, tier);

        }
        storePatients();
        storeReferrals();
        storeContacts();
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

    private Item createReferral(String patientId, String referralId, String age, String locality,
                                String diagnosis, String urgency, String source, String outcome, String referralDate,
                                String triageDate, String assessmentDate, String firstTreatmentDate,
                                String dischargeDate, String dischargeReason, String cumulativeCAMHS)
            throws ObjectStoreException {

        String patRefId = patientId + "-" + referralId;  // to identify the referral
//        LOG.info("PATREF = " + patRefId);
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

            Item patient = patients.get(patientId);
            if (patient != null) {
                item.setReference("patient", patient);
            }
            referrals.put(patRefId, item);
        }
        return item;
    }

    private Item createContact(String patientId, String referralId, String contactId,
                               String ordinal, String contactDate, String urgency,
                               String contactType, String attendance, String outcome, String team, String tier)
            throws ObjectStoreException {

        if (patientId == null) {
            patientId = ref2pat.get(referralId);
        }
        String patRefId = patientId + "-" + referralId;  // to identify the referral/contact
    //    LOG.info("PATREF CON " + patRefId);

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

    private Item createDiagnostic(String patientId, String referralId, String assessmentDate, String observation,
                                  String value)
            throws ObjectStoreException {
        String patRefId = patientId + "-" + referralId;

        Item referral = referrals.get(patRefId);
        Item patient = patients.get(patientId);
        Item dia = createItem("Diagnostic");
        dia.setAttributeIfNotNull("assessmentDate", assessmentDate);
        dia.setAttributeIfNotNull("observation", observation);
        dia.setAttributeIfNotNull("value", value);
        if (patient != null) {
            dia.setReference("patient", patient);
        }
        if (referral != null) {
            dia.setReference("referral", referral);
        }
        return dia;
    }

    private Item createAdditionalData(String patientId, String referralId, String type, String name,
                                  String value)
            throws ObjectStoreException {
        Item patient = patients.get(patientId);
        Item ad = createItem(type);
        ad.setAttributeIfNotNull("name", name);
        ad.setAttributeIfNotNull("value", value);
        if (patient != null) {
            ad.setReference("patient", patient);
        }
        if (referralId != null) {
            String patRefId = patientId + "-" + referralId;
            Item referral = referrals.get(patRefId);
            if (referral != null) {
                ad.setReference("referral", referral);
            }
        }
        return ad;
    }


    /**
     * create datasource and dataset
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

    private String cleanIdentifier(String identifier) {
        // stockport, e.g.:
        // patientId = RT2550527
        // referralId = 3_155204910
        // contactId = 5_C_2480976  (activity Id)
        //
        // worcester:
        // contactId = 1150471DA
        //
        // waltham:
        // patientId = 1022464RiO
        // referralId = 1022464MHRef1 (NB: we could be missing info in the last digit)
        //

        if (identifier.contains("NULL")) {
            return null;
        }
        if (identifier.startsWith("RT")) {
            return identifier.replace("RT", "");
        }
        if (identifier.startsWith("TH")) {
            return identifier.replace("TH", "");
        }
        if (identifier.startsWith("LT")) {
            return identifier.replace("LT", "");
        }
        if (identifier.startsWith("RMY")) {
            return identifier.replace("RMY", "");
        }
        if (identifier.contains("_")) {
            String[] splitted = identifier.split("_");
            return splitted[splitted.length - 1];
        }
        if (identifier.endsWith("DA")) {
            return identifier.replace("DA", "");
        }
        if (identifier.endsWith("CA")) {
            return identifier.replace("CA", "");
        }
        if (identifier.endsWith("GA")) {
            return identifier.replace("GA", "");
        }
        if (identifier.endsWith("RiO")) { // waltham patientid
            return identifier.replace("RiO", "");
        }
        if (identifier.contains("MH")) { // waltham referralid
            return identifier.substring(0, identifier.indexOf('M'));
        }

        return identifier;
    }

    private void setDataset(String fileName) throws ObjectStoreException {
        if (fileName.contains("Bexley")) {
            dataSet = "Bexley";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Cambridge")) {
            dataSet = "Cambridge and Peterborough";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Camden")) {
            dataSet = "Camden";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Hertfordshire")) {
            dataSet = "Hertfordshire";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Lewisham")) {
            siteType = SITE_CONTROL;
            dataSet = "Lewisham";
        }
        if (fileName.contains("Luton")) {
            dataSet = "Luton and Tower Hamlet";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Manchester")) {
            dataSet = "Manchester and Salford";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("NeCor")) {
            siteType = SITE_CONTROL;
            dataSet = "Nene and Corby";
        }
        if (fileName.contains("Norfolk")) {
            dataSet = "Norfolk";
            siteType = SITE_CONTROL;
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
        if (fileName.contains("Stoke")) {
            dataSet = "Stoke on Trent";
            siteType = SITE_CONTROL;
        }
        if (fileName.contains("Sunderland")) {
            dataSet = "Sunderland";
            siteType = SITE_CONTROL;
        }
        if (fileName.contains("Waltham")) {
            dataSet = "Waltham Forest";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Warrington")) {
            dataSet = "Warrington";
            siteType = SITE_ITHRIVE;
        }
        if (fileName.contains("Worcester")) {
            dataSet = "Worcester";
            siteType = SITE_CONTROL;
        }

        createDataSet(dataSet, siteType);
    }

}
