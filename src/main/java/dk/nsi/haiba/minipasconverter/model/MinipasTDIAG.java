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
package dk.nsi.haiba.minipasconverter.model;

import java.util.Date;

public class MinipasTDIAG {
    private int v_recnum;
    private String idnummer;
    private String c_diag;
    private String c_diagtype;
    private String c_tildiag;
    private Date indberetningsdato;

    public int getV_recnum() {
        return v_recnum;
    }

    public void setV_recnum(int v_recnum) {
        this.v_recnum = v_recnum;
    }

    public String getIdnummer() {
        return idnummer;
    }

    public void setIdnummer(String idnummer) {
        this.idnummer = idnummer;
    }

    public String getC_diag() {
        return c_diag;
    }

    public void setC_diag(String c_diag) {
        this.c_diag = c_diag;
    }

    public String getC_diagtype() {
        return c_diagtype;
    }

    public void setC_diagtype(String c_diagtype) {
        this.c_diagtype = c_diagtype;
    }

    public String getC_tildiag() {
        return c_tildiag;
    }

    public void setC_tildiag(String c_tildiag) {
        this.c_tildiag = c_tildiag;
    }

    public Date getIndberetningsdato() {
        return indberetningsdato;
    }

    public void setIndberetningsdato(Date indberetningsdato) {
        this.indberetningsdato = indberetningsdato;
    }
}
