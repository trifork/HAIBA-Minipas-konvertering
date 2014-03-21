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

public class MinipasTSKSUBE_OPR implements MinipasRowWithRecnum {
    private int v_recnum;
    private String idnummer;
    private String c_tilopr;
    private String c_oprart;
    private Date d_odto;
    private String c_osgh;
    private String c_oafd;
    private String c_opr;
    private Date indberetningsdato;
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
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

    public String getC_tilopr() {
        return c_tilopr;
    }

    public void setC_tilopr(String c_tilopr) {
        this.c_tilopr = c_tilopr;
    }

    public String getC_oprart() {
        return c_oprart;
    }

    public void setC_oprart(String c_oprart) {
        this.c_oprart = c_oprart;
    }

    public Date getD_odto() {
        return d_odto;
    }

    public void setD_odto(Date d_odto) {
        this.d_odto = d_odto;
    }

    public String getC_osgh() {
        return c_osgh;
    }

    public void setC_osgh(String c_osgh) {
        this.c_osgh = c_osgh;
    }

    public String getC_oafd() {
        return c_oafd;
    }

    public void setC_oafd(String c_oafd) {
        this.c_oafd = c_oafd;
    }

    public String getC_opr() {
        return c_opr;
    }

    public void setC_opr(String c_opr) {
        this.c_opr = c_opr;
    }

    public Date getIndberetningsdato() {
        return indberetningsdato;
    }

    public void setIndberetningsdato(Date indberetningsdato) {
        this.indberetningsdato = indberetningsdato;
    }
}
