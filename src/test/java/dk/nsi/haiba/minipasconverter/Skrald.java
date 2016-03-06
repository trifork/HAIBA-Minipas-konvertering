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
package dk.nsi.haiba.minipasconverter;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.SimpleTriggerContext;

public class Skrald {
    public static void mai23n(String[] args) {
        Date d = new Date();
        System.out.println(d);
    }
    
    public static void main(String[] args) {
        CronTrigger trigger = new CronTrigger("0 0 3 * * *");
        SimpleTriggerContext context = new SimpleTriggerContext();
        Date nextExecutionTime = trigger.nextExecutionTime(context);
        System.out.println(nextExecutionTime);
    }
    
    public static void maffin(String[] args) {
        Map<Integer, Object> map = generateMap(100000);
        m1(map);
        m2(map);
    }

    private static Map<Integer, Object> generateMap(int count) {
        Map<Integer, Object> map = new HashMap<Integer, Object>();
        Random r = new Random();
        while (map.size() < count) {
            map.put(r.nextInt(), new Object());
        }
        return map;
    }

    public static void m1(Map<Integer, Object> map) {
        long time = System.currentTimeMillis();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        Set<Integer> keySet = map.keySet();
        for (Integer integer : keySet) {
            min = Math.min(min, integer);
            max = Math.max(max, integer);
        }
        long time2 = System.currentTimeMillis();
        System.out.println(time2 - time);
        System.out.println(max);
        System.out.println(min);
    }
    public static void m2(Map<Integer, Object> map) {
        long time = System.currentTimeMillis();
        Integer min = Collections.min(map.keySet());
        int max = Collections.max(map.keySet());
        
        long time2 = System.currentTimeMillis();
        System.out.println(time2 - time);
        System.out.println(max);
        System.out.println(min);
    }
}
