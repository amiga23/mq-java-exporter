package ru.cinimex.exporter.mq;

import static com.ibm.mq.constants.MQConstants.MQCACH_CHANNEL_NAME;
import static com.ibm.mq.constants.MQConstants.MQCACH_LISTENER_NAME;
import static com.ibm.mq.constants.MQConstants.MQCA_Q_NAME;
import static com.ibm.mq.constants.MQConstants.MQCMD_INQUIRE_CHANNEL_STATUS;
import static com.ibm.mq.constants.MQConstants.MQCMD_INQUIRE_LISTENER_STATUS;
import static com.ibm.mq.constants.MQConstants.MQCMD_INQUIRE_Q;
import static com.ibm.mq.constants.MQConstants.MQCMD_INQUIRE_Q_STATUS;
import static com.ibm.mq.constants.MQConstants.MQIACH_CHANNEL_STATUS;
import static com.ibm.mq.constants.MQConstants.MQIACH_LISTENER_STATUS;
import static com.ibm.mq.constants.MQConstants.MQIA_MAX_Q_DEPTH;
import static com.ibm.mq.constants.MQConstants.MQIA_CURRENT_Q_DEPTH;
import static com.ibm.mq.constants.MQConstants.MQIA_INHIBIT_GET;
import static com.ibm.mq.constants.MQConstants.MQIA_INHIBIT_PUT;
import static com.ibm.mq.constants.MQConstants.MQIA_Q_TYPE;
import static com.ibm.mq.constants.MQConstants.MQIACF_OLDEST_MSG_AGE;
import static com.ibm.mq.constants.MQConstants.MQQT_LOCAL;
import static com.ibm.mq.constants.MQConstants.MQIA_OPEN_INPUT_COUNT; // Consumer - POV application
import static com.ibm.mq.constants.MQConstants.MQIA_OPEN_OUTPUT_COUNT; // Producer
import static com.ibm.mq.constants.MQConstants.MQIA_MSG_ENQ_COUNT;
import static com.ibm.mq.constants.MQConstants.MQIA_MSG_DEQ_COUNT;
import static com.ibm.mq.constants.MQConstants.MQCACF_LAST_PUT_TIME;
import static com.ibm.mq.constants.MQConstants.MQCACF_LAST_GET_TIME;
import static com.ibm.mq.constants.MQConstants.MQCACF_LAST_PUT_DATE;
import static com.ibm.mq.constants.MQConstants.MQCACF_LAST_GET_DATE;

import com.ibm.mq.headers.pcf.PCFMessage;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.util.Pair;

/**
 * Class represents MQObject (Queue, channel or listener). It stores object type and all PCFParameters, required for
 * correct request.
 */
public class MQObject {

    private static final Logger logger = LogManager.getLogger(MQObject.class);
    private final String name;
    private final MQType type;
    private final List<Pair<Integer, String>> pcfHeadersToMetricMappings;
    private final PCFMessage pcfCmd;
    /**
     * MQObject constructor.
     *
     * @param name - object name.
     * @param type - object type.
     */
    public MQObject(String name, MQType type) {
        this.name = name;
        this.type = type;
        this.pcfHeadersToMetricMappings = new ArrayList<>();

/*
 * PCF commands are used to retrieve some specific statistics from queue manager.
 */
        switch (type) {
            case QUEUE:
                pcfCmd = new PCFMessage(MQCMD_INQUIRE_Q); //if object type is queue, exporter would inquire it.
                pcfCmd.addParameter(MQCA_Q_NAME,
                    name); //PCF command would try to retrieve statistics about queue with specific name
                pcfCmd.addParameter(MQIA_Q_TYPE, MQQT_LOCAL); // and specific type
                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_MAX_Q_DEPTH, "mqobject_queue_queue_max_depth_messages"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_INHIBIT_PUT, "mqobject_queue_queue_put_inhibited_untyped"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_INHIBIT_GET, "mqobject_queue_queue_get_inhibited_untyped"));
                // @TODO already in as mqobject_queue_queue_depth_messages but defined somewhere else!
                // pcfHeadersToMetricMappings.add(new Pair<>(MQIA_CURRENT_Q_DEPTH, "mqobject_queue_queue_current_depth_messages"));

                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_OPEN_INPUT_COUNT, "mqobject_queue_queue_open_input_count"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_OPEN_OUTPUT_COUNT, "mqobject_queue_queue_open_output_count"));
//                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_MSG_ENQ_COUNT, "mqobject_queue_queue_enqueed_messages"));
//                pcfHeadersToMetricMappings.add(new Pair<>(MQIA_MSG_DEQ_COUNT, "mqobject_queue_queue_dequeed_messages"));
                break;
            case QUEUE_STATUS:
                pcfCmd = new PCFMessage(MQCMD_INQUIRE_Q_STATUS); //if object type is queue, exporter would inquire it.
                pcfCmd.addParameter(MQCA_Q_NAME,
                    name); //PCF command would try to retrieve statistics about queue with specific name
                pcfCmd.addParameter(MQIA_Q_TYPE, MQQT_LOCAL); // and specific type
                pcfHeadersToMetricMappings.add(new Pair<>(MQIACF_OLDEST_MSG_AGE, "mqobject_queue_queue_oldest_message_age"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQCACF_LAST_PUT_TIME, "mqobject_queue_queue_last_put_time"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQCACF_LAST_GET_TIME, "mqobject_queue_queue_last_get_time"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQCACF_LAST_PUT_DATE, "mqobject_queue_queue_last_put_date"));
                pcfHeadersToMetricMappings.add(new Pair<>(MQCACF_LAST_GET_DATE, "mqobject_queue_queue_last_get_date"));
                break;
            case LISTENER:
                pcfCmd = new PCFMessage(
                    MQCMD_INQUIRE_LISTENER_STATUS); //if object type is listener, exporter would inquire it.
                pcfCmd.addParameter(MQCACH_LISTENER_NAME,
                    name);//PCF command would try to retrieve statistics about listener with specific name
                pcfHeadersToMetricMappings
                    .add(new Pair<>(MQIACH_LISTENER_STATUS, "mqobject_listener_listener_status_untyped")); //the only statistics we want to know about listener is it's status.
                break;
            case CHANNEL:
                pcfCmd = new PCFMessage(
                    MQCMD_INQUIRE_CHANNEL_STATUS); //if object type is channel, exporter would inquire it.
                pcfCmd.addParameter(MQCACH_CHANNEL_NAME,
                    name); //PCF command would try to retrieve statistics about channel with specific name
                pcfHeadersToMetricMappings
                    .add(new Pair<>(MQIACH_CHANNEL_STATUS, "mqobject_channel_channel_status_untyped")); //the only statistics we want to know about channel is it's status.
                break;
            default:
                logger.error("Unknown type for MQObject: {}", type.name());
                throw new RuntimeException(
                    "Unable to create new MQObject. Received unexpected MQObject type: " + type.name());
        }
    }

    /**
     * This method returns MQConstant int code, which represents name for input object.
     *
     * @param type - object type.
     * @return - integer code. Returns -1 if code wasn't found.
     */
    public static int objectNameCode(MQObject.MQType type) {
        int code = -1;
        switch (type) {
            case QUEUE:
            case QUEUE_STATUS:
                code = MQCA_Q_NAME;
                break;
            case CHANNEL:
                code = MQCACH_CHANNEL_NAME;
                break;
            case LISTENER:
                code = MQCACH_LISTENER_NAME;
                break;
        }
        return code;
    }


    /**
     * Getter for object name.
     *
     * @return object name.
     */
    public String getName() {
        return name;
    }

    /**
     * Getter for PCFHeaders mappings.
     *
     * @return - list with mapping headers to metrics.
     */
    public List<Pair<Integer, String>> getPcfHeadersToMetricMappings() {
        return pcfHeadersToMetricMappings;
    }

    /**
     * Getter for object type.
     *
     * @return object type.
     */
    public MQType getType() {
        return type;
    }

    /**
     * Getter for PCF command.
     *
     * @return - prepared PCF command object.
     */
    public PCFMessage getPcfCmd() {
        return pcfCmd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MQObject mqObject = (MQObject) o;

        if (!name.equals(mqObject.name)) return false;
        return type == mqObject.type;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    /**
     * This enum represents all supported MQObject types.
     */
    public enum MQType {QUEUE, QUEUE_STATUS, CHANNEL, LISTENER}
}
