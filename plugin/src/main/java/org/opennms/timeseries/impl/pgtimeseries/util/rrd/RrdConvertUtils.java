/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2013-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/
package org.opennms.timeseries.impl.pgtimeseries.util.rrd;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.sax.SAXSource;

import org.jrobin.core.RrdDb;
import org.jrobin.core.RrdException;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.v1.RRDv1;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.v3.RRDv3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * The Class RRD Conversion Utilities.
 *
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class RrdConvertUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RrdConvertUtils.class);

    /** How much of rrdtool's stderr is kept for an error message. */
    private static final int STDERR_LIMIT = 4000;

    // JAXBContext creation is expensive (it introspects the whole class graph) and the context is thread safe,
    // so one per schema is shared by every import thread. Unmarshallers are not thread safe: one per call.
    private static volatile JAXBContext rrdV1Context;
    private static volatile JAXBContext rrdV3Context;

    /**
     * Instantiates a new RRDtool Convert Utils.
     */
    private RrdConvertUtils() {}

    private static JAXBContext v1Context() throws JAXBException {
        JAXBContext c = rrdV1Context;
        if (c == null) {
            synchronized (RrdConvertUtils.class) {
                c = rrdV1Context;
                if (c == null) {
                    rrdV1Context = c = JAXBContext.newInstance(RRDv1.class);
                }
            }
        }
        return c;
    }

    private static JAXBContext v3Context() throws JAXBException {
        JAXBContext c = rrdV3Context;
        if (c == null) {
            synchronized (RrdConvertUtils.class) {
                c = rrdV3Context;
                if (c == null) {
                    rrdV3Context = c = JAXBContext.newInstance(RRDv3.class);
                }
            }
        }
        return c;
    }

    /**
     * Dumps a JRB.
     *
     * @param sourceFile the source file
     * @return the RRD Object (old version)
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws RrdException the RRD exception
     */
    public static RRDv1 dumpJrb(File sourceFile) throws IOException, RrdException, JAXBException {
        final RrdDb jrb = new RrdDb(sourceFile, true);
        try {
            final Unmarshaller jaxbUnmarshaller = v1Context().createUnmarshaller();
            return (RRDv1) jaxbUnmarshaller.unmarshal(new StringReader(jrb.getXml()));
        } finally {
            jrb.close();
        }
    }

    /**
     * Dumps a RRD.
     *
     * @param sourceFile the source file
     * @return the RRD Object
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws RrdException the RRD exception
     */
    public static RRDv3 dumpRrd(File sourceFile) throws IOException, RrdException {
        final String rrdBinary = System.getProperty("rrd.binary");
        if (rrdBinary == null) {
            throw new IllegalArgumentException("rrd.binary property must be set");
        }
        final Process process = new ProcessBuilder(rrdBinary, "dump", sourceFile.getAbsolutePath()).start();
        // stderr must be drained concurrently: an rrdtool that fills the pipe while we are still reading stdout
        // would block forever, and so would we.
        final StderrDrain stderr = new StderrDrain(process.getErrorStream());
        stderr.start();
        try {
            final XMLReader xmlReader = XMLReaderFactory.createXMLReader();
            xmlReader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            final SAXSource source = new SAXSource(xmlReader,
                    new InputSource(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)));
            final RRDv3 rrd = (RRDv3) v3Context().createUnmarshaller().unmarshal(source);
            final int exit = process.waitFor();
            if (exit != 0) {
                throw new RrdException("rrdtool dump of " + sourceFile + " exited with status " + exit + stderr.suffix());
            }
            return rrd;
        } catch (RrdException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RrdException("Interrupted while dumping " + sourceFile, e);
        } catch (Exception e) {
            throw new RrdException("Can't parse the RRD dump of " + sourceFile + stderr.suffix(), e);
        } finally {
            process.destroy(); // no-op once it has exited; kills a dump we gave up on
        }
    }

    /** Reads a process's stderr to the end, keeping the first {@link #STDERR_LIMIT} characters for diagnostics. */
    private static final class StderrDrain extends Thread {
        private final InputStream in;
        private final StringBuilder text = new StringBuilder();

        StderrDrain(final InputStream in) {
            super("rrdtool-stderr");
            this.in = in;
            setDaemon(true);
        }

        @Override
        public void run() {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    synchronized (text) {
                        if (text.length() < STDERR_LIMIT) {
                            text.append(line).append('\n');
                        }
                    }
                }
            } catch (IOException e) {
                LOG.debug("rrdtool stderr closed", e);
            }
        }

        /** The captured stderr formatted for appending to a message, or an empty string if there was none. */
        String suffix() {
            try {
                join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            synchronized (text) {
                final String s = text.toString().trim();
                return s.isEmpty() ? "" : " (rrdtool said: " + s + ")";
            }
        }
    }
}