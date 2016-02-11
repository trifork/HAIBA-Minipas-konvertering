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

public class MinipasTBES implements MinipasRowWithRecnum {
    private int v_recnum; // ud efter kopi, nyt nummer hver nat
    private String idnummer; // 38 guid
    private Date d_ambdto;
    private Date indberetningsdato;

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

    public Date getD_ambdto() {
        return d_ambdto;
    }

    public void setD_ambdto(Date d_ambdto) {
        this.d_ambdto = d_ambdto;
    }

    public Date getIndberetningsdato() {
        return indberetningsdato;
    }

    public void setIndberetningsdato(Date indberetningsdato) {
        this.indberetningsdato = indberetningsdato;
    }
}
