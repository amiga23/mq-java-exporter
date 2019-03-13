package ru.cinimex.exporter.mq.pcf;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.pcf.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;

/**
 * Class PCFDataParser contains only static methods and was created to simplify work with PCF messages.
 */
public class PCFDataParser {
    private static final Logger logger = LogManager.getLogger(PCFDataParser.class);

    /**
     * Method parses PCFMessage, which contains info about all monitoring classes
     *
     * @param pcfMessage - message, which was published to SYS/MQ/INFO/QMGR/{QMGR_NAME}/Monitor/METADATA/CLASSES
     * @return Array, filled with parsed data (Each element represents monitoring class)
     */
    public static ArrayList<PCFClass> getPCFClasses(PCFMessage pcfMessage) {
        Enumeration<PCFParameter> params = pcfMessage.getParameters();
        ArrayList<PCFClass> classes = new ArrayList<PCFClass>();
        while (params.hasMoreElements()) {
            com.ibm.mq.pcf.PCFParameter param = params.nextElement();
            switch (param.getParameter()) {
                case MQConstants.MQGACF_MONITOR_CLASS: {
                    Enumeration<com.ibm.mq.pcf.PCFParameter> groupParams = ((MQCFGR) param).getParameters();
                    int monitorId = -1;
                    int monitorFlag = -1;
                    String monitorName = null;
                    String monitorDesc = null;
                    String topicString = null;
                    while (groupParams.hasMoreElements()) {
                        PCFParameter groupParam = groupParams.nextElement();
                        switch (groupParam.getParameter()) {
                            case MQConstants.MQIAMO_MONITOR_CLASS:
                                monitorId = (Integer) groupParam.getValue();
                                break;
                            case MQConstants.MQIAMO_MONITOR_FLAGS:
                                monitorFlag = (Integer) groupParam.getValue();
                                break;
                            case MQConstants.MQCAMO_MONITOR_CLASS:
                                monitorName = groupParam.getStringValue();
                                break;
                            case MQConstants.MQCAMO_MONITOR_DESC:
                                monitorDesc = groupParam.getStringValue();
                                break;
                            case MQConstants.MQCA_TOPIC_STRING:
                                topicString = groupParam.getStringValue();
                                break;
                            default:
                                logger.warn("Unknown parameter type was found while parsing PCFClass! Will be ignored. {} = {}", groupParam.getParameterName(), groupParam.getStringValue());
                                break;
                        }

                    }
                    classes.add(new PCFClass(monitorId, monitorFlag, monitorName, monitorDesc, topicString));
                }
            }

        }
        return classes;
    }

    /**
     * Method parses PCFMessage, which contains info about all monitoring types for specific class
     *
     * @param pcfMessage - message, which was published to $SYS/MQ/INFO/QMGR/{QMGR_NAME}/Monitor/METADATA/{CLASS}/TYPES
     * @return Array, filled with parsed data (Each element represents monitoring type)
     */
    public static ArrayList<PCFType> getPCFTypes(PCFMessage pcfMessage) {
        Enumeration<PCFParameter> params = pcfMessage.getParameters();
        ArrayList<PCFType> types = new ArrayList<PCFType>();
        while (params.hasMoreElements()) {
            PCFParameter param = params.nextElement();
            switch (param.getParameter()) {
                case MQConstants.MQGACF_MONITOR_TYPE: {
                    Enumeration<PCFParameter> groupParams = ((MQCFGR) param).getParameters();
                    String monitorName = null;
                    String monitorDesc = null;
                    String topicString = null;
                    while (groupParams.hasMoreElements()) {
                        PCFParameter groupParam = groupParams.nextElement();
                        switch (groupParam.getParameter()) {
                            case MQConstants.MQCAMO_MONITOR_TYPE:
                                monitorName = groupParam.getStringValue();
                                break;
                            case MQConstants.MQCAMO_MONITOR_DESC:
                                monitorDesc = groupParam.getStringValue();
                                break;
                            case MQConstants.MQCA_TOPIC_STRING:
                                topicString = groupParam.getStringValue();
                                break;
                            default:
                                logger.warn("Unknown parameter type was found while parsing PCFType! Will be ignored." + " {} = {}", groupParam.getParameterName(), groupParam.getStringValue());
                                break;
                        }

                    }
                    types.add(new PCFType(monitorName, monitorDesc, topicString));
                }
            }
        }
        return types;
    }

    /**
     * Method parses PCFMessage, which contains info about all monitoring elements for specific type
     *
     * @param pcfMessage - message, which was published to $SYS/MQ/INFO/QMGR/{QMGR_NAME}/Monitor/{CLASS}/{TYPE}
     * @return Array, filled with parsed data (Each element represents monitoring element)
     */
    public static ArrayList<PCFElement> getPCFElements(PCFMessage pcfMessage) {
        Enumeration<PCFParameter> params = pcfMessage.getParameters();
        ArrayList<PCFElement> elements = new ArrayList<PCFElement>();
        ArrayList<PCFElementRow> rows = new ArrayList<PCFElementRow>();
        while (params.hasMoreElements()) {
            PCFParameter param = params.nextElement();
            String topicString = null;
            switch (param.getParameter()) {
                case MQConstants.MQGACF_MONITOR_ELEMENT: {
                    Enumeration<com.ibm.mq.pcf.PCFParameter> groupParams = ((MQCFGR) param).getParameters();
                    int rowId = -1;
                    int rowDatatype = -1;
                    String rowDesc = null;
                    while (groupParams.hasMoreElements()) {
                        PCFParameter groupParam = groupParams.nextElement();
                        switch (groupParam.getParameter()) {
                            case MQConstants.MQIAMO_MONITOR_ELEMENT:
                                rowId = (Integer) groupParam.getValue();
                                break;
                            case MQConstants.MQIAMO_MONITOR_DATATYPE:
                                rowDatatype = (Integer) groupParam.getValue();
                                break;
                            case MQConstants.MQCAMO_MONITOR_DESC:
                                rowDesc = groupParam.getStringValue();
                                break;
                            default:
                                logger.warn("Unknown parameter type was found while parsing PCFElement! Will be " + "ignored." + " {} = {}", groupParam.getParameterName(), groupParam.getStringValue());
                                break;
                        }

                    }
                    rows.add(new PCFElementRow(rowId, rowDatatype, rowDesc));
                }
                break;
                case MQConstants.MQCA_TOPIC_STRING:
                    topicString = param.getStringValue();
                    break;
                default:
            }
            if (topicString != null && rows.size() > 0) {
                elements.add(new PCFElement(topicString, rows));
            }
        }
        return elements;
    }

    /**
     * Converts a message that is expected to contain specific statistical values.
     *
     * @param pcfMessage - input PCF message with statistic data
     * @return - HashMap, where Integer identifier is corellated with header from PCFElement.
     */
    public static HashMap<Integer, Double> getParsedData(PCFMessage pcfMessage) {
        Enumeration<PCFParameter> params = pcfMessage.getParameters();
        HashMap<Integer, Double> data = new HashMap<Integer, Double>();
        while (params.hasMoreElements()) {
            PCFParameter param = params.nextElement();
            switch (param.getParameter()) {
                case MQConstants.MQCA_Q_MGR_NAME:
                    break;
                case MQConstants.MQIAMO_MONITOR_CLASS:
                    break;
                case MQConstants.MQIAMO_MONITOR_TYPE:
                    // Should perhaps check that it's as expected
                    break;
                case MQConstants.MQIAMO64_MONITOR_INTERVAL:
                    // Monitor interval, i.e. time since last publish, is in milliseconds
                    break;
                case MQConstants.MQIACF_OBJECT_TYPE:
                    break;
                case MQConstants.MQCA_Q_NAME:
                    break;
                default:
                    switch (param.getType()) {
                        case (MQConstants.MQCFT_INTEGER): {
                            MQCFIN statistic = (MQCFIN) param;
                            data.put(statistic.getParameter(), new Double(statistic.getIntValue()));
                            break;
                        }
                        case (MQConstants.MQCFT_INTEGER64): {
                            MQCFIN64 statistic = (MQCFIN64) param;
                            data.put(statistic.getParameter(), new Double(statistic.getLongValue()));
                            break;
                        }
                        default: {
                            logger.warn("Unknown parameter type was found while parsing PCF monitoring data! Will be " + "ignored." + " {} = {}", param.getParameterName(), param.getStringValue());
                            break;
                        }
                    }
            }

        }
        return data;
    }

    /**
     * Converts MQMessage to PCFMessage
     *
     * @param message - input MQMessage
     * @return - converted PCFMessage
     */
    public static PCFMessage convertToPCF(MQMessage message) {
        try {
            return new PCFMessage(message);
        } catch (MQException | IOException e) {
            logger.error("Unable to convert MQMessage to PCFMessage: ", e);
        }
        return null;
    }
}
