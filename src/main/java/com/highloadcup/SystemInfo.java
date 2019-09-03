package com.highloadcup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

public class SystemInfo {

    private static final Logger logger = LogManager.getLogger(SystemInfo.class);


    private static ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private static OperatingSystemMXBean opBean = ManagementFactory.getOperatingSystemMXBean();


    public static void logSystemInfo() throws Exception {
        Process proc = Runtime.getRuntime().exec("uname -a");

        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

        //BufferedReader stdError = new BufferedReader(new
        //        InputStreamReader(proc.getErrorStream()));

        String s = null;
        while ((s = stdInput.readLine()) != null) {
            logger.error("uname -a: {}", s);
        }

        proc = Runtime.getRuntime().exec("lscpu");

        stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

        //BufferedReader stdError = new BufferedReader(new
        //        InputStreamReader(proc.getErrorStream()));

        s = null;
        while ((s = stdInput.readLine()) != null) {
            if ((s.contains("x86") || s.contains("Intel("))
                && !s.contains("fpu")
            ) {
                logger.error("lscpu: {}", s);
            }
        }

        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

        logger.error("load avg - {}", operatingSystemMXBean.getSystemLoadAverage());
        logger.error("avail procs - {}", operatingSystemMXBean.getAvailableProcessors());

        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        //ThreadInfo info = threadBean.getThreadInfo(Thread.currentThread().getId());

        logger.error("getThreadCount: {}", threadBean.getThreadCount());

        logger.error("getCurrentThreadCpuTime: {}", threadBean.getCurrentThreadCpuTime());
        logger.error("getCurrentThreadUserTime: {}", threadBean.getCurrentThreadUserTime());
        logger.error("isThreadCpuTimeSupported: {}", threadBean.isThreadCpuTimeSupported());
        logger.error("isCurrentThreadCpuTimeSupported: {}", threadBean.isCurrentThreadCpuTimeSupported());
        logger.error("isThreadCpuTimeEnabled: {}", threadBean.isThreadCpuTimeEnabled());
    }

    public static void printUsage() {
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

        List<GarbageCollectorMXBean> garbageCollectorMXBeans =  ManagementFactory.getGarbageCollectorMXBeans();

        garbageCollectorMXBeans.forEach( gc -> {
            logger.error("gc {}, count - {}, time - {}", gc.getClass(), gc.getCollectionCount(), gc.getCollectionTime());
        });
        //logger.error("*********************************************************");
        logger.error("Heap free: {}", Runtime.getRuntime().freeMemory());
        //logger.error("load avg - {}", operatingSystemMXBean.getSystemLoadAverage());

        /*
        //Method[] methods = operatingSystemMXBean.getClass().getDeclaredMethods();
        for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if ((method.getName().contains("Cpu") || method.getName().contains("System"))
                    && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (Exception e) {
                    value = e;
                } // try
                logger.error(method.getName() + " = " + value);
            } // if
        } // for
        logger.error("*********************************************************");
        */
    }


}
