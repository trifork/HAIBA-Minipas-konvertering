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
package dk.nsi.haiba.minipasconverter.status;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CurrentImportProgress {
    private DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    private StringBuffer sb;
    private Object aMutex = new Object();
    private boolean aProgressDot;
    private int aLineDotCount;

    public void reset() {
        synchronized (aMutex) {
            sb = new StringBuffer();
        }
    }

    public void addStatusLine(String status) {
        synchronized (aMutex) {
            if (aProgressDot) {
                // break line to end progress
                sb.append("<br/>");
            }
            sb.append(dateFormat.format(new Date()) + " " + status + "<br/>");
            aProgressDot = false;
        }
    }

    public void addProgressDot() {
        synchronized (aMutex) {
            aProgressDot = true;
            sb.append(".");
            aLineDotCount++;
            if (aLineDotCount == 100) {
                aLineDotCount = 0;
                sb.append("<br>");
            }
        }
    }

    public String getStatus() {
        String returnValue = null;
        synchronized (aMutex) {
            returnValue = sb != null ? sb.toString() : "(no current import status)";
        }
        return returnValue;
    }
}
