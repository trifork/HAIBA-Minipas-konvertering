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
package dk.nsi.haiba.minipasconverter.dao;

import java.util.Collection;
import java.util.List;

import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTBES;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;

public interface MinipasDAO {
    /**
     * @param year full year with century, e.g. 2014
     * @param fromKRecnum recnum to import from, not inclusive
     * @param batchSize number of results
     * @return
     */
    public Collection<MinipasTADM> getMinipasTADM(String year, long fromKRecnum, int batchSize);

    public Collection<MinipasTSKSUBE_OPR> getMinipasSKSUBE_OPR(String year, int recnum);

    public Collection<MinipasTDIAG> getMinipasDIAG(String year, int recnum);

    public Collection<MinipasTBES> getMinipasBES(String year, int recnum);

    public long lastReturnCodeElseNegativeOne();

    void reset();

    List<MinipasTADM> getMinipasTADMForIdnummer(int year, List<MinipasTADM> minipasTADMFromHaiba);

    List<MinipasTBES> getMinipasTBESForIdnummer(int year, List<MinipasTADM> minipasTADMFromHaiba);

}