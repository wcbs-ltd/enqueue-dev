<?php

declare(strict_types=1);

namespace Enqueue\RdKafka;

use Interop\Queue\Consumer;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Message;
use Interop\Queue\Queue;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;

class RdKafkaConsumer implements Consumer
{
    use SerializerAwareTrait;

    /**
     * @var KafkaConsumer
     */
    private $consumer;

    /**
     * @var RdKafkaContext
     */
    private $context;

    /**
     * @var RdKafkaTopic
     */
    private $topic;

    private bool $subscribed = false;

    private bool $commitAsync = true;

    /**
     * @var int|null
     */
    private $offset;

    public function __construct(KafkaConsumer $consumer, RdKafkaContext $context, RdKafkaTopic $topic, Serializer $serializer)
    {
        $this->setConsumer($consumer);
        $this->setContext($context);
        $this->setTopic($topic);

        $this->setSerializer($serializer);
    }

    public function isCommitAsync(): bool
    {
        return $this->commitAsync;
    }

    public function setCommitAsync(bool $async): void
    {
        $this->commitAsync = $async;
    }

    public function getOffset(): ?int
    {
        return $this->offset;
    }

    public function setOffset(?int $offset = null): void
    {
        if ($this->isSubscribed()) {
            throw new \LogicException('The consumer has already subscribed.');
        }

        $this->offset = $offset;
    }

    /**
     * @return RdKafkaTopic
     */
    public function getQueue(): Queue
    {
        return $this->getTopic();
    }

    /**
     * @return RdKafkaMessage
     */
    public function receive(int $timeout = 0): ?Message
    {
        if (false === $this->isSubscribed()) {
            if (null === $this->getOffset()) {
                $this->getConsumer()->subscribe([$this->getQueue()->getQueueName()]);
            } else {
                $this->getConsumer()->assign([new TopicPartition(
                    $this->getQueue()->getQueueName(),
                    $this->getQueue()->getPartition(),
                    $this->getOffset()
                )]);
            }

            $this->subscribed();
        }

        if ($timeout > 0) {
            return $this->doReceive($timeout);
        }

        while (true) {
            if ($message = $this->doReceive(500)) {
                return $message;
            }
        }

        return null;
    }

    /**
     * @return RdKafkaMessage
     */
    public function receiveNoWait(): ?Message
    {
        throw new \LogicException('Not implemented');
    }

    /**
     * @param RdKafkaMessage $message
     */
    public function acknowledge(Message $message): void
    {
        InvalidMessageException::assertMessageInstanceOf($message, RdKafkaMessage::class);

        if (false == $message->getKafkaMessage()) {
            throw new \LogicException('The message could not be acknowledged because it does not have kafka message set.');
        }

        if ($this->isCommitAsync()) {
            $this->getConsumer()->commitAsync($message->getKafkaMessage());
        } else {
            $this->getConsumer()->commit($message->getKafkaMessage());
        }
    }

    /**
     * @param RdKafkaMessage $message
     */
    public function reject(Message $message, bool $requeue = false): void
    {
        $this->acknowledge($message);

        if ($requeue) {
            $this->getContext()->createProducer()->send($this->getTopic(), $message);
        }
    }

    protected function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    protected function subscribed(): void
    {
        $this->subscribed = true;
    }

    protected function setConsumer(KafkaConsumer $consumer): void
    {
        $this->consumer = $consumer;
    }

    protected function getConsumer(): KafkaConsumer
    {
        return $this->consumer;
    }

    protected function getContext(): RdKafkaContext
    {
        return $this->context;
    }

    protected function setContext(RdKafkaContext $context): void
    {
        $this->context = $context;
    }

    protected function getTopic(): RdKafkaTopic
    {
        return $this->topic;
    }

    protected function setTopic(RdKafkaTopic $topic): void
    {
        $this->topic = $topic;
    }

    private function doReceive(int $timeout): ?RdKafkaMessage
    {
        $kafkaMessage = $this->getConsumer()->consume($timeout);

        if (null === $kafkaMessage) {
            return null;
        }

        switch ($kafkaMessage->err) {
            case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case \RD_KAFKA_RESP_ERR__TIMED_OUT:
            case \RD_KAFKA_RESP_ERR__TRANSPORT:
                return null;
            case \RD_KAFKA_RESP_ERR_NO_ERROR:
                $message = $this->serializer->toMessage($kafkaMessage->payload);
                $message->setKey($kafkaMessage->key);
                $message->setPartition($kafkaMessage->partition);
                $message->setKafkaMessage($kafkaMessage);

                // Merge headers passed from Kafka with possible earlier serialized payload headers. Prefer Kafka's.
                // Note: Requires phprdkafka >= 3.1.0
                if (isset($kafkaMessage->headers)) {
                    $message->setHeaders(array_merge($message->getHeaders(), $kafkaMessage->headers));
                }

                return $message;
            default:
                throw new \LogicException($kafkaMessage->errstr(), $kafkaMessage->err);
                break;
        }
    }
}
