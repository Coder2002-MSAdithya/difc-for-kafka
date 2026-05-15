package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;
import org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap;

import java.lang.reflect.Method;

public class KafkaEntrypointAdvice
{
    @Advice.OnMethodEnter
    public static void enter()
    {
        SocketPolicyBootstrap.enterTrusted();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void exit()
    {
        SocketPolicyBootstrap.exitTrusted();
    }

    public static class StreamSourceAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) Object source)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl("stream"))
            {
                return;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=from(" + source + ")");
        }
    }

    public static class FromAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter(@Advice.Argument(0) Object topics)
        {
            if (!StreamsDslAttestation.enterOperator("from"))
            {
                return false;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=from(" + normalizeTopics(topics) + ")");
            return true;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(@Advice.Enter boolean emitted)
        {
            if (emitted)
            {
                StreamsDslAttestation.exitOperator("from");
            }
        }
    }

    public static String normalizeTopics(Object topics)
    {
        if (topics == null)
        {
            return "null";
        }

        String s = String.valueOf(topics);

        if (s.startsWith("[") && s.endsWith("]"))
        {
            s = s.substring(1, s.length() - 1);
        }

        return s;
    }

    public static class FilterAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object predicate)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "filter"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "filter",
                    predicate);
        }
    }

    public static class MapValuesAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "mapValues"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "mapValues",
                    mapper);
        }
    }

    public static class MapAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "map"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "map",
                    mapper);
        }
    }

    public static class FlatMapAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "flatMap"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "flatMap",
                    mapper);
        }
    }

    public static class FlatMapValuesAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "flatMapValues"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "flatMapValues",
                    mapper);
        }
    }

    public static class SelectKeyAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "selectKey"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "selectKey",
                    mapper);
        }
    }

    public static class GroupByAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object mapper)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "groupBy"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "groupBy",
                    mapper);
        }
    }

    public static class GroupByKeyAdvice
    {
        @Advice.OnMethodEnter
        public static void enter()
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "groupByKey"))
            {
                return;
            }

            System.out.println(
                    "[POLICY][ATTEST] topology.statement=groupByKey()");
        }
    }

    public static class ReduceAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object reducer)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "reduce"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "reduce",
                    reducer);
        }
    }

    public static class AggregateAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(1)
                Object aggregator)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "aggregate"))
            {
                return;
            }

            StreamsDslAttestation.attest(
                    "aggregate",
                    aggregator);
        }
    }

    public static class CountAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            if (!StreamsDslAttestation
                    .enterOperator("count"))
            {
                return false;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=count()");

            return true;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(
                @Advice.Enter boolean emitted)
        {
            if (emitted)
            {
                StreamsDslAttestation
                        .exitOperator("count");
            }
        }
    }

    public static class WindowedByAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object windows)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "windowedBy"))
            {
                return;
            }

            try
            {
                Class<?> cls =
                        windows.getClass();

                Method sizeMethod =
                        cls.getMethod("sizeMs");

                Method graceMethod =
                        cls.getMethod("gracePeriodMs");

                Object size =
                        sizeMethod.invoke(windows);

                Object grace =
                        graceMethod.invoke(windows);

                System.out.println(
                        "[POLICY][ATTEST] topology.statement=windowedBy("
                                + cls.getSimpleName()
                                + "[sizeMs="
                                + size
                                + ",graceMs="
                                + grace
                                + "])");
            }
            catch (Throwable ignored)
            {
                System.out.println(
                        "[POLICY][ATTEST] topology.statement=windowedBy("
                                + windows
                                + ")");
            }
        }
    }

    public static class ToStreamAdvice
    {
        @Advice.OnMethodEnter
        public static void enter()
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "toStream"))
            {
                return;
            }

            System.out.println(
                    "[POLICY][ATTEST] topology.statement=toStream()");
        }
    }

    public static class ToAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object topic)
        {
            if (!StreamsDslAttestation
                    .shouldEmitUserDsl(
                            "to"))
            {
                return;
            }

            if (topic != null &&
                    topic.toString().contains(
                            "StaticTopicNameExtractor"))
            {
                return;
            }

            System.out.println(
                    "[POLICY][ATTEST] topology.statement=to("
                            + topic
                            + ")");
        }
    }
}