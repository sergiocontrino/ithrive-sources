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

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.sql.Database;
import org.intermine.xml.full.Item;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author
 */
public class IthrivedbConverter extends BioDBConverter
{
    // 
    private static final String DATASET_TITLE = "iThriveDB";
    private static final String DATA_SOURCE_NAME = "NHS";


    /**
     * Construct a new IthrivedbConverter.
     * @param database the database to read from
     * @param model the Model used by the object store we will write to with the ItemWriter
     * @param writer an ItemWriter used to handle Items created
     */
    public IthrivedbConverter(Database database, Model model, ItemWriter writer) {
        super(database, model, writer, DATA_SOURCE_NAME, DATASET_TITLE);
    }


    /**
     * {@inheritDoc}
     */
    public void process() throws Exception {
        // a database has been initialised from properties starting with db.ithrivedb

        Connection connection = getDatabase().getConnection();

        // process data with direct SQL queries on the source database, for example:
        
        // Statement stmt = connection.createStatement();
        // String query = "select column from table;";
        // ResultSet res = stmt.executeQuery(query);
        // while (res.next()) {
        // }   
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }

    @Override
    public String getLicence() {
        return null;
    }
}
