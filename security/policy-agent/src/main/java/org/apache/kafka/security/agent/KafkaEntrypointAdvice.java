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

    public static class TableAdvice
    {
        @Advice.OnMethodEnter
        public static Object[] enter(@Advice.Argument(0) Object topics)
        {
            if (!StreamsDslAttestation.enterOperator("table"))
            {
                return new Object[]{false, topics};
            }

            System.out.println("[POLICY][ATTEST] topology.statement=table(" + normalizeTopics(topics) + ")");
            return new Object[]{true, topics};
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(@Advice.Enter Object[] state, @Advice.Return Object returned)
        {
            if ((boolean) state[0])
            {
                StreamsDslAttestation.exitOperator("table");
                DslGraphTracker.recordTableSource(returned, state[1]);
            }
        }
    }

    public static class FromAdvice
    {
        @Advice.OnMethodEnter
        public static Object[] enter(@Advice.Argument(0) Object topics)
        {
            if (!StreamsDslAttestation.enterOperator("from"))
            {
                return new Object[]{false, topics};
            }

            System.out.println("[POLICY][ATTEST] topology.statement=from(" + normalizeTopics(topics) + ")");
            return new Object[]{true, topics};
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(@Advice.Enter Object[] state, @Advice.Return Object returned)
        {
            if ((boolean) state[0])
            {
                StreamsDslAttestation.exitOperator("from");
                DslGraphTracker.recordSource(returned, state[1]);
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
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("filter");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object predicate, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("filter", predicate);
            DslGraphTracker.recordUnary("filter", stream, returned, null, predicate, false, false);
        }
    }

    public static class MapValuesAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("mapValues");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("mapValues", mapper);
            DslGraphTracker.recordUnary("mapValues", stream, returned, null, mapper, false, false);
            DslProcessingPolicyTracker.recordMapValuesProjection(stream, returned, mapper);
        }
    }

    public static class ProcessAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter(@Advice.Argument(0) Object processorSupplier)
        {
            if (!org.apache.kafka.security.agent.policy.ProcessorOutputAnalyzer.isUserProvidedProcessorSupplier(
                    processorSupplier))
            {
                return false;
            }
            return StreamsDslAttestation.shouldEmitUserDsl("process");
        }

        @Advice.OnMethodExit
        public static void exit(
                @Advice.Enter boolean emit,
                @Advice.This Object stream,
                @Advice.Argument(0) Object processorSupplier,
                @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }
            StreamsDslAttestation.attest("process", processorSupplier);
            DslGraphTracker.recordUnary("process", stream, returned, null, processorSupplier, false, false);
            DslProcessingPolicyTracker.recordProcessProjection(stream, returned, processorSupplier);
        }
    }

    public static class MapAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("map");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("map", mapper);
            DslGraphTracker.recordUnary("map", stream, returned, null, mapper, false, false);
        }
    }

    public static class FlatMapAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("flatMap");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("flatMap", mapper);
            DslGraphTracker.recordUnary("flatMap", stream, returned, null, mapper, false, false);
        }
    }

    public static class FlatMapValuesAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("flatMapValues");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("flatMapValues", mapper);
            DslGraphTracker.recordUnary("flatMapValues", stream, returned, null, mapper, false, false);
        }
    }

    public static class SelectKeyAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("selectKey");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("selectKey", mapper);
            DslGraphTracker.recordUnary("selectKey", stream, returned, null, mapper, false, false);
        }
    }

    public static class GroupByAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("groupBy");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object mapper, @Advice.Return Object returned)
        {
            if (!emit) return;
            StreamsDslAttestation.attest("groupBy", mapper);
            DslGraphTracker.recordUnary("groupBy", stream, returned, null, mapper, false, false);
        }
    }

    public static class GroupByKeyAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("groupByKey");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=groupByKey()");
            DslGraphTracker.recordUnary("groupByKey", stream, returned, null, null, false, false);
        }
    }

    public static class ReduceAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("reduce");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object reducer, @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            StreamsDslAttestation.attest("reduce", reducer);
            DslGraphTracker.recordUnary("reduce", stream, returned, null, reducer, false, false);
        }
    }

    public static class AggregateAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("aggregate");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(1) Object aggregator, @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            StreamsDslAttestation.attest("aggregate", aggregator);
            DslGraphTracker.recordUnary("aggregate", stream, returned, null, aggregator, false, false);
        }
    }

    public static class CountAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            if (!StreamsDslAttestation.enterOperator("count"))
            {
                return false;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=count()");
            return true;
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(@Advice.Enter boolean emitted, @Advice.This Object stream, @Advice.Return Object returned)
        {
            if (emitted)
            {
                StreamsDslAttestation.exitOperator("count");
                DslGraphTracker.recordUnary("count", stream, returned, null, null, false, false);
            }
        }
    }

    public static class WindowedByAdvice
    {
        @Advice.OnMethodEnter
        public static Object[] enter(@Advice.Argument(0) Object windows)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl("windowedBy"))
            {
                return new Object[]{false, windows, String.valueOf(windows)};
            }

            try
            {
                Class<?> cls = windows.getClass();
                Method sizeMethod = cls.getMethod("sizeMs");
                Method graceMethod = cls.getMethod("gracePeriodMs");

                Object size = sizeMethod.invoke(windows);
                Object grace = graceMethod.invoke(windows);
                String semantics = cls.getSimpleName() + "[sizeMs=" + size + ",graceMs=" + grace + "]";

                System.out.println("[POLICY][ATTEST] topology.statement=windowedBy(" + semantics + ")");
                return new Object[]{true, windows, semantics};
            }
            catch (Throwable ignored)
            {
                String semantics = String.valueOf(windows);
                System.out.println("[POLICY][ATTEST] topology.statement=windowedBy(" + semantics + ")");
                return new Object[]{true, windows, semantics};
            }
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter Object[] state, @Advice.This Object stream, @Advice.Return Object returned)
        {
            if (!(boolean) state[0])
            {
                return;
            }

            DslGraphTracker.recordUnary("windowedBy", stream, returned, state[2], null, false, false);
        }
    }

    public static class JoinAdvice
    {
        @Advice.OnMethodEnter
        public static Object[] enter(@Advice.Origin("#m") String method, @Advice.Argument(0) Object other, @Advice.AllArguments Object[] args)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl(method))
            {
                return new Object[]{false, method, other, null};
            }

            Object joiner = args != null && args.length > 1 ? args[1] : null;
            System.out.println("[POLICY][ATTEST] topology.statement=" + method + "(...)");
            if (joiner != null)
            {
                StreamsDslAttestation.attest(method, joiner);
            }
            return new Object[]{true, method, other, joiner};
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter Object[] state, @Advice.This Object left, @Advice.Return Object returned)
        {
            if (!(boolean) state[0])
            {
                return;
            }
            DslGraphTracker.recordJoin((String) state[1], left, state[2], returned, state[3]);
        }
    }

    public static class MergeAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation
                    .shouldEmitUserDsl("merge");
        }

        @Advice.OnMethodExit
        public static void exit(
                @Advice.Enter boolean emit,
                @Advice.This Object left,
                @Advice.Argument(0) Object right,
                @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            DslGraphTracker.recordJoin(
                    "merge",
                    left,
                    right,
                    returned,
                    null);
        }
    }

    public static class ThroughAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation
                    .shouldEmitUserDsl("through");
        }

        @Advice.OnMethodExit
        public static void exit(
                @Advice.Enter boolean emit,
                @Advice.This Object stream,
                @Advice.Argument(0) Object topic,
                @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            DslGraphTracker.recordThrough(
                    stream,
                    returned,
                    topic);
        }
    }

    public static class SplitAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation
                    .shouldEmitUserDsl("split");
        }

        @Advice.OnMethodExit
        public static void exit(
                @Advice.Enter boolean emit,
                @Advice.This Object stream,
                @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            DslGraphTracker.recordUnary(
                    "split",
                    stream,
                    returned,
                    null,
                    null,
                    false,
                    false);
        }
    }

    public static class BranchAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("branch");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Argument(0) Object predicates, @Advice.Return Object[] branches)
        {
            if (!emit)
            {
                return;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=branch(...)");
            DslGraphTracker.recordBranch(stream, branches, predicates);
        }
    }

    public static class InternalTopicAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) String topic)
        {
            System.out.println("[POLICY][ATTEST] internal.topic=" + topic);
            DslGraphTracker.recordInternalTopic(topic);
        }
    }

    public static class StateStoreAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) Object store)
        {
            System.out.println("[POLICY][ATTEST] state.store=" + store);
            DslGraphTracker.recordStateStore(store);
        }
    }

    public static class ProcessorAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) String name)
        {
            System.out.println("[POLICY][ATTEST] processor=" + name);
        }
    }

    public static class ToStreamAdvice
    {
        @Advice.OnMethodEnter
        public static boolean enter()
        {
            return StreamsDslAttestation.shouldEmitUserDsl("toStream");
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.Enter boolean emit, @Advice.This Object stream, @Advice.Return Object returned)
        {
            if (!emit)
            {
                return;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=toStream()");
            DslGraphTracker.recordUnary("toStream", stream, returned, null, null, false, false);
        }
    }

    public static class ToAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.This Object stream, @Advice.Argument(0) Object topic)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl("to"))
            {
                return;
            }

            System.out.println("[POLICY][ATTEST] topology.statement=to(" + topic + ")");
            DslGraphTracker.recordUnary("to", stream, null, topic, null, false, true);
            DslProcessingPolicyTracker.recordSinkPolicy(stream, topic);
        }
    }

    public static class AddTagsAdvice
    {
        @Advice.OnMethodExit
        public static void exit(
                @Advice.This Object stream,
                @Advice.Argument(0) Object tags,
                @Advice.Return Object returned)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl("addTags"))
            {
                return;
            }
            System.out.println("[POLICY][ATTEST] topology.statement=addTags(" + tags + ")");
            DslProcessingPolicyTracker.recordDifcAddTags(stream, tags, returned);
        }
    }

    public static class DeclassifyTagsAdvice
    {
        @Advice.OnMethodExit
        public static void exit(
                @Advice.This Object stream,
                @Advice.Argument(0) Object tags,
                @Advice.Return Object returned)
        {
            if (!StreamsDslAttestation.shouldEmitUserDsl("declassifyTags"))
            {
                return;
            }
            System.out.println("[POLICY][ATTEST] topology.statement=declassifyTags(" + tags + ")");
            DslProcessingPolicyTracker.recordDifcDeclassifyTags(stream, tags, returned);
        }
    }

    public static class ProducerSendAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) Object record)
        {
            AppClientPolicyTracker.recordProducerSend(record);
            org.apache.kafka.security.agent.policy.EgressFieldObserver.observeProducerRecord(record);
        }
    }

    public static class ConsumerSubscribeAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) Object topics)
        {
            AppClientPolicyTracker.recordConsumerSubscribe(topics);
        }
    }

    public static class ConsumerAssignAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(@Advice.Argument(0) Object partitions)
        {
            AppClientPolicyTracker.recordConsumerAssign(partitions);
        }
    }
}