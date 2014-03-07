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

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class ImportStatus {
	private DateTime startTime;
	private DateTime endTime;
	private Outcome outcome;
	private String errorMessage;

	public DateTime getStartTime() {
		return startTime;
	}

	public void setStartTime(DateTime startTime) {
		this.startTime = startTime;
	}

	public DateTime getEndTime() {
		return endTime;
	}

	public void setEndTime(DateTime endTime) {
		this.endTime = endTime;
	}

	public Outcome getOutcome() {
		return outcome;
	}

	public void setOutcome(Outcome outcome) {
		this.outcome = outcome;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public static enum Outcome {
		SUCCESS,
		FAILURE
	}

	@Override
	public String toString() {
		String body = "\nLast import started at: " + this.getStartTime();
		Outcome outcome = this.getOutcome();

		if (endTime != null) {
			body += " and ended at: " + this.getEndTime();
			body += ". Processing took " + new Duration(this.getStartTime(), this.getEndTime()).getStandardSeconds() + " seconds";
		} else {
			body += " and is still running";
		}

		if (outcome != null) {
			body += ". Outcome was " + outcome;
			if(outcome.equals(Outcome.FAILURE)) {
				body += ", error was " +errorMessage;
			}
		}

		return body;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ImportStatus that = (ImportStatus) o;

		if (endTime != null ? !endTime.equals(that.endTime) : that.endTime != null) return false;
		if (outcome != that.outcome) return false;
		if (startTime != null ? !startTime.equals(that.startTime) : that.startTime != null) return false;
		if (errorMessage != null ? !errorMessage.equals(that.errorMessage) : that.errorMessage != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = startTime != null ? startTime.hashCode() : 0;
		result = 31 * result + (endTime != null ? endTime.hashCode() : 0);
		result = 31 * result + (outcome != null ? outcome.hashCode() : 0);
		result = 31 * result + (errorMessage != null ? errorMessage.hashCode() : 0);
		return result;
	}

}
