package ru.cinimex.exporter.prometheus.metrics;

import io.prometheus.client.CollectorRegistry;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * Util class is used to update metrics.
 */
public class MetricManagerUtils {
    private MetricManagerUtils() {
    }


    /**
     * @return - list of metric names, than will be updated
     */
    public static List<String> getUpdatedMetricNames() {
        List<String> updatedMetricNames = new ArrayList<>();
        updatedMetricNames.add("mqobject_get_average_destructive_mqget_persistent_message_size_bytes");
        updatedMetricNames.add("mqobject_get_average_destructive_mqget_non_persistent_message_size_bytes");
        updatedMetricNames.add("mqobject_get_average_destructive_mqget_persistent_and_non_persistent_message_size_bytes");
        updatedMetricNames.add("mqobject_queue_queue_fill_percentage");
        // @TODO ???? What's this method doing?
        //System.out.println(">>>>>>>>> TKS >>>>> ");
        //updatedMetricNames.add("mqobject_queue_queue_enqueed_messages");
        //System.out.println(">>>>>>>>> TKS 2 >>>>> ");
        return updatedMetricNames;
    }


    /**
     * @param updatedMetricName - metric name, than will be updated
     * @return list of metric names, than will be used to update metric with name updatedMetricName
     */
    public static List<String> getMetricsNamesUsedToUpdate(String updatedMetricName) {
        List<String> listWithNames = new ArrayList<>();
        switch (updatedMetricName) {
            case "mqobject_get_average_destructive_mqget_persistent_message_size_bytes":
                listWithNames.add("mqobject_get_destructive_mqget_persistent_byte_count_totalbytes");
                listWithNames.add("mqobject_get_destructive_mqget_persistent_message_count_totalmessages");
                break;
            case "mqobject_get_average_destructive_mqget_non_persistent_message_size_bytes":
                listWithNames.add("mqobject_get_destructive_mqget_non_persistent_byte_count_totalbytes");
                listWithNames.add("mqobject_get_destructive_mqget_non_persistent_message_count_totalmessages");
                break;
            case "mqobject_get_average_destructive_mqget_persistent_and_non_persistent_message_size_bytes":
                listWithNames.add("mqobject_get_destructive_mqget_persistent_byte_count_totalbytes");
                listWithNames.add("mqobject_get_destructive_mqget_non_persistent_byte_count_totalbytes");
                listWithNames.add("mqobject_get_destructive_mqget_persistent_message_count_totalmessages");
                listWithNames.add("mqobject_get_destructive_mqget_non_persistent_message_count_totalmessages");
                break;
            case "mqobject_queue_queue_fill_percentage":
                listWithNames.add("mqobject_queue_queue_depth_messages");
                listWithNames.add("mqobject_queue_queue_max_depth_messages");
                break;
            default:
                break;
        }
        return listWithNames;
    }

    /**
     * @param updatedMetricName - metric name, than will be updated
     * @return function, that will be used for updating metric value
     */
    public static Function<List<Double>, Double> getConversionFunction(String updatedMetricName) {
        switch (updatedMetricName) {
            case "mqobject_get_average_destructive_mqget_persistent_message_size_bytes":
            case "mqobject_get_average_destructive_mqget_non_persistent_message_size_bytes":
                return MetricManagerUtils::division;
            case "mqobject_get_average_destructive_mqget_persistent_and_non_persistent_message_size_bytes":
                return MetricManagerUtils::averageSum;
            case "mqobject_queue_queue_fill_percentage":
                return MetricManagerUtils::divisionInPercentage;
            default:
                return MetricManagerUtils::defaultConversion;
        }
    }

    private static Double averageSum(List<Double> params) {
        Objects.requireNonNull(params);
        if (params.size() != 4) throw new IllegalArgumentException();
        return (params.get(2) + params.get(3)) == 0.0 ? 0.0 : (params.get(0) + params.get(1)) / (params.get(2) + params.get(3));
    }

    private static Double division(List<Double> params) {
        Objects.requireNonNull(params);
        if (params.size() != 2) throw new IllegalArgumentException();
        return params.get(1) == 0.0 ? 0.0 : params.get(0) / params.get(1);
    }

    private static Double divisionInPercentage(List<Double> params) {
        Objects.requireNonNull(params);
        if (params.size() != 2) throw new IllegalArgumentException();
        return params.get(1) == 0.0 ? 0.0 : params.get(0) / (params.get(1) * 100);
    }

    private static Double defaultConversion(List<Double> params) {
        return Objects.requireNonNull(params).get(0);
    }

    /**
     * @param parsedQuery       - parameter, needed for getting metric in special family metrics
     * @param updatedMetricName - metric name, that will be updated
     * @return map with label list as key and double parameter list used for conversion function as value
     */
    public static Map<List<String>, List<Double>> getMetricsUsedToUpdate(Set<String> parsedQuery, String updatedMetricName) {
        List<String> metricsNamesUsedToUpdate = getMetricsNamesUsedToUpdate(updatedMetricName);
        Map<String, Map<List<String>, Double>> mapWithValues = new HashMap<>();
        forEachRemaining(CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(parsedQuery), metricFamilySamples -> {
            Map<List<String>, Double> arrayListDoubleMap =
                    metricFamilySamples.samples.stream()
                            .filter(sample -> metricsNamesUsedToUpdate.contains(sample.name))
                            .collect(toMap(sample -> sample.labelValues, sample -> sample.value, (a, b) -> b));
            if (!arrayListDoubleMap.isEmpty()) mapWithValues.put(metricFamilySamples.name, arrayListDoubleMap);
        });

        Map<List<String>, List<Double>> params = new HashMap<>();

        if (mapWithValues.size() == metricsNamesUsedToUpdate.size()) {
            List<Map<List<String>, Double>> listWithValues = metricsNamesUsedToUpdate.stream()
                    .map(mapWithValues::get)
                    .collect(Collectors.toList());
            listWithValues.forEach(l -> l.forEach((k, v) -> {
                        if (!params.containsKey(k)) params.put(k, new ArrayList<>(Collections.singletonList(v)));
                        else {
                            List<Double> paramList = params.get(k);
                            paramList.add(v);
                            params.replace(k, paramList);
                        }
                    }
            ));
        }
        return params;
    }

    private static <T> void forEachRemaining(Enumeration<T> e, Consumer<? super T> c) {
        while (e.hasMoreElements()) c.accept(e.nextElement());
    }
}
