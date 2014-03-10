/**
 * The MIT License
 *
 * Original work sponsored and donated by National Board of e-Health (NSI), Denmark
 * (http://www.nsi.dk)
 *
 * Copyright (C) 2011 National Board of e-Health (NSI), Denmark (http://www.nsi.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package dk.nsi.haiba.minipasconverter.executor;

import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasSyncDAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasSyncDAO.MinipasSyncStructure;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;

public class MinipasPreprocessor {
    private static Logger aLog = Logger.getLogger(MinipasPreprocessor.class);

    @Value("${minipas.batchsize:!00}")
    int batchSize;

    @Autowired
    MinipasDAO minipasDao;

    @Autowired
    MinipasSyncDAO minipasSyncDao;

    @Autowired
    MinipasHAIBADAO haibaDao;

    private boolean manualOverride;
    private boolean running;

    @Scheduled(cron = "${cron.import.job}")
    public void run() {
        if (running) {
            if (!isManualOverride()) {
                aLog.debug("Running Importer: " + new Date().toString());
                doProcess(false);
            } else {
                aLog.debug("Importer must be started manually");
            }
        } else {
            aLog.debug("already running");
        }
    }

    public void doProcess(boolean manual) {
        aLog.info("Started processing, manual=" + manual);

        int kRecnum = -1;
        int currentyear = getYear();
        int yearsback = 5;
        try {
            running = true;
            if (minipasDao.isDatabaseReadyForImport() != 0) {
                // cleanup - will only cleanup the sync db, not the data already converted and imported
                minipasSyncDao.cleanupRowsFromTablesOlderThanYear(currentyear - yearsback + 1);
                haibaDao.importStarted();
                for (int year = currentyear - yearsback + 1; year <= currentyear; year++) {
                    String sYear = "" + year;
                    Collection<MinipasTADM> minipasTADM = null;
                    while (minipasTADM == null || !minipasTADM.isEmpty()) {
                        minipasTADM = minipasDao.getMinipasTADM(sYear, kRecnum, batchSize);
                        MinipasSyncStructure syncStructure = minipasSyncDao.test(year, minipasTADM);

                        handleCreated(sYear, syncStructure.getCreated());
                        handleUpdated(sYear, syncStructure.getUpdated());

                        // now remember that we have processed the changes
                        minipasSyncDao.commit(year, syncStructure);

                        kRecnum = getMaxRecnum(minipasTADM);
                    }

                    // ask sync dao what we haven't mentioned yet - they are deleted
                    Collection<String> deleted = minipasSyncDao.getDeletedIdnummers(year);
                    handleDeleted(sYear, deleted);
                }
            } else {
                String status = "MINIPAS is not ready for import, previous job is not finished yet.";
                aLog.warn(status);
            }
        } finally {
            running = false;
        }
    }

    private void handleDeleted(String year, Collection<String> deletedIdnummers) {
        for (String idnummer : deletedIdnummers) {
            haibaDao.clearAdm(idnummer);
            haibaDao.clearKoder(idnummer);
            haibaDao.setDeleted(idnummer);
        }
    }

    private void handleUpdated(String year, Collection<MinipasTADM> updated) {
        haibaDao.resetAdmD_IMPORTDTO(updated);
        for (MinipasTADM minipasTADM : updated) {
            haibaDao.clearKoder(minipasTADM.getIdnummer());
            Collection<MinipasTSKSUBE_OPR> ubeoprs = minipasDao.getMinipasSKSUBE_OPR(year, minipasTADM.getIdnummer());
            haibaDao.createKoderFromSksUbeOpr(minipasTADM, ubeoprs);
            Collection<MinipasTDIAG> diags = minipasDao.getMinipasDIAG(year, minipasTADM.getIdnummer());
            haibaDao.createKoderFromDiag(minipasTADM, diags);
        }
    }

    private void handleCreated(String year, Collection<MinipasTADM> created) {
        haibaDao.createAdm(created);
        for (MinipasTADM minipasTADM : created) {
            Collection<MinipasTSKSUBE_OPR> ubeoprs = minipasDao.getMinipasSKSUBE_OPR(year, minipasTADM.getIdnummer());
            haibaDao.createKoderFromSksUbeOpr(minipasTADM, ubeoprs);
            Collection<MinipasTDIAG> diags = minipasDao.getMinipasDIAG(year, minipasTADM.getIdnummer());
            haibaDao.createKoderFromDiag(minipasTADM, diags);
        }
    }

    private int getMaxRecnum(Collection<MinipasTADM> minipasTADMs) {
        int returnValue = -1;
        for (MinipasTADM minipasTADM : minipasTADMs) {
            returnValue = Math.max(returnValue, minipasTADM.getK_recnum());
        }
        return returnValue;
    }

    /**
     * Returns year with century, e.g. 2014
     */
    private int getYear() {
        return new DateTime().getYear();
    }

    public boolean isManualOverride() {
        return manualOverride;
    }

    public void setManualOverride(boolean mo) {
        manualOverride = mo;
    }

    public void doManualProcess() {
        doProcess(true);
    }
}