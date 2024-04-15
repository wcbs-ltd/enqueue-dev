<?php

declare(strict_types=1);

namespace Enqueue\RdKafka;

use Interop\Queue\Consumer;
use Interop\Queue\Context;
use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\PurgeQueueNotSupportedException;
use Interop\Queue\Exception\SubscriptionConsumerNotSupportedException;
use Interop\Queue\Exception\TemporaryQueueNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;
use Interop\Queue\Queue;
use Interop\Queue\SubscriptionConsumer;
use Interop\Queue\Topic;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer as VendorProducer;

class RdKafkaContext implements Context
{
    use SerializerAwareTrait;

    /**
     * @var array
     */
    private $config;

    /**
     * @var Conf
     */
    private $conf;

    /**
     * @var RdKafkaProducer
     */
    private $producer;

    /**
     * @var KafkaConsumer[]
     */
    private $kafkaConsumers = [];

    /**
     * @var RdKafkaConsumer[]
     */
    private $rdKafkaConsumers = [];

    public function __construct(array $config)
    {
        $this->setConfig($config);

        $this->setSerializer(new JsonSerializer());
    }

    /**
     * @return RdKafkaMessage
     */
    public function createMessage(string $body = '', array $properties = [], array $headers = []): Message
    {
        return new RdKafkaMessage($body, $properties, $headers);
    }

    /**
     * @return RdKafkaTopic
     */
    public function createTopic(string $topicName): Topic
    {
        return new RdKafkaTopic($topicName);
    }

    /**
     * @return RdKafkaTopic
     */
    public function createQueue(string $queueName): Queue
    {
        return new RdKafkaTopic($queueName);
    }

    public function createTemporaryQueue(): Queue
    {
        throw TemporaryQueueNotSupportedException::providerDoestNotSupportIt();
    }

    /**
     * @return RdKafkaProducer
     */
    public function createProducer(): Producer
    {
        if (!isset($this->producer)) {
            $producer = new VendorProducer($this->getConf());

            if (isset($this->getConfig()['log_level'])) {
                $producer->setLogLevel($this->getConfig()['log_level']);
            }

            $this->producer = new RdKafkaProducer($producer, $this->getSerializer());

            // Once created RdKafkaProducer can store messages internally that need to be delivered before PHP shuts
            // down. Otherwise, we are bound to lose messages in transit.
            // Note that it is generally preferable to call "close" method explicitly before shutdown starts, since
            // otherwise we might not have access to some objects, like database connections.
            register_shutdown_function([$this->producer, 'flush'], $this->getConfig()['shutdown_timeout'] ?? -1);
        }

        return $this->producer;
    }

    /**
     * @param RdKafkaTopic $destination
     *
     * @return RdKafkaConsumer
     */
    public function createConsumer(Destination $destination): Consumer
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, RdKafkaTopic::class);

        $queueName = $destination->getQueueName();

        if (!$this->rdKafkaConsumerExists($queueName)) {
            $kafkaConsumer = new KafkaConsumer($this->getConf());

            $this->appendKafkaConsumer($kafkaConsumer);

            $consumer = new RdKafkaConsumer(
                $kafkaConsumer,
                $this,
                $destination,
                $this->getSerializer()
            );

            if (isset($this->getConfig()['commit_async'])) {
                $consumer->setCommitAsync($this->getConfig()['commit_async']);
            }

            $this->appendRdKafkaConsumer($consumer, $queueName);
        }

        return $this->getRdKafkaConsumer($queueName);
    }

    public function close(): void
    {
        $kafkaConsumers = $this->kafkaConsumers;
        $this->kafkaConsumers = [];
        $this->rdKafkaConsumers = [];

        foreach ($kafkaConsumers as $kafkaConsumer) {
            $kafkaConsumer->unsubscribe();
        }

        // Compatibility with phprdkafka 4.0.
        if (isset($this->producer)) {
            $this->producer->flush($this->getConfig()['shutdown_timeout'] ?? -1);
        }
    }

    public function createSubscriptionConsumer(): SubscriptionConsumer
    {
        throw SubscriptionConsumerNotSupportedException::providerDoestNotSupportIt();
    }

    public function purgeQueue(Queue $queue): void
    {
        throw PurgeQueueNotSupportedException::providerDoestNotSupportIt();
    }

    public static function getLibrdKafkaVersion(): string
    {
        if (!defined('RD_KAFKA_VERSION')) {
            throw new \RuntimeException('RD_KAFKA_VERSION constant is not defined. Phprdkafka is probably not installed');
        }
        $major = (\RD_KAFKA_VERSION & 0xFF000000) >> 24;
        $minor = (\RD_KAFKA_VERSION & 0x00FF0000) >> 16;
        $patch = (\RD_KAFKA_VERSION & 0x0000FF00) >> 8;

        return "$major.$minor.$patch";
    }

    public function setConfig(array $config): void
    {
        $this->config = $config;
    }

    public function rdKafkaConsumerExists(string $queueName): bool
    {
        return isset($this->rdKafkaConsumers[$queueName]);
    }

    public function appendKafkaConsumer(KafkaConsumer $kafkaConsumer): void
    {
        $this->kafkaConsumers[] = $kafkaConsumer;
    }

    public function appendRdKafkaConsumer(RdKafkaConsumer $consumer, string $queueName): void
    {
        $this->rdKafkaConsumers[$queueName] = $consumer;
    }

    public function getRdKafkaConsumer(string $queueName)
    {
        return $this->rdKafkaConsumers[$queueName];
    }

    protected function getConf(): Conf
    {
        if (null === $this->conf) {
            $this->conf = new Conf();

            if (isset($this->getConfig()['topic']) && is_array($this->getConfig()['topic'])) {
                foreach ($this->getConfig()['topic'] as $key => $value) {
                    $this->conf->set($key, $value);
                }
            }

            if (isset($this->getConfig()['partitioner'])) {
                $this->conf->set('partitioner', $this->getConfig()['partitioner']);
            }

            if (isset($this->getConfig()['global']) && is_array($this->getConfig()['global'])) {
                foreach ($this->getConfig()['global'] as $key => $value) {
                    $this->conf->set($key, $value);
                }
            }

            if (isset($this->getConfig()['dr_msg_cb'])) {
                $this->conf->setDrMsgCb($this->getConfig()['dr_msg_cb']);
            }

            if (isset($this->getConfig()['error_cb'])) {
                $this->conf->setErrorCb($this->getConfig()['error_cb']);
            }

            if (isset($this->getConfig()['rebalance_cb'])) {
                $this->conf->setRebalanceCb($this->getConfig()['rebalance_cb']);
            }

            if (isset($this->getConfig()['stats_cb'])) {
                $this->conf->setStatsCb($this->getConfig()['stats_cb']);
            }
        }

        return $this->conf;
    }

    protected function getConfig(): array
    {
        return $this->config;
    }
}
