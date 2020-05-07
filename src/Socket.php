<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\core\Loop;
use rabbit\kafka\Protocol\Protocol;
use function feof;
use function fread;
use function is_resource;
use function stream_set_blocking;
use function stream_set_read_buffer;
use function strlen;
use function substr;

/**
 * Class Socket
 * @package rabbit\kafka
 */
class Socket extends CommonSocket
{
    /**
     * @var string
     */
    private $writeBuffer = '';

    /**
     * @var string
     */
    private $readBuffer = '';

    /**
     * @var int
     */
    private $readNeedLength = 0;

    /**
     * @var callable|null
     */
    private $onReadable;

    public function connect(): void
    {
        if (!$this->isSocketDead()) {
            return;
        }

        $this->createStream();

        stream_set_blocking($this->stream, false);
        stream_set_read_buffer($this->stream, 0);

        Loop::addEvent('kafka', [
            $this->stream,
            function (): void {
                $newData = @fread($this->stream, self::READ_MAX_LENGTH);

                if ($newData) {
                    $this->read($newData);
                }
            },
            null,
            SWOOLE_EVENT_READ
        ]);
    }

    public function reconnect(): void
    {
        $this->logger->warning("fd=" . (int)$this->stream . ' is dead!', ['module' => 'Kafka']);
        $this->close();
        $this->connect();
        $this->logger->info("fd=" . (int)$this->stream . ' reconnected!', ['module' => 'Kafka']);
    }

    public function setOnReadable(callable $read): void
    {
        $this->onReadable = $read;
    }

    public function close(): void
    {
        Loop::stopEvent('kafka', (int)$this->stream, true);

        $this->readBuffer = '';
        $this->writeBuffer = '';
        $this->readNeedLength = 0;
    }

    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @param string|int $data
     */
    public function read($data): void
    {
        $this->readBuffer .= (string)$data;

        do {
            if ($this->readNeedLength === 0) { // response start
                if (strlen($this->readBuffer) < 4) {
                    return;
                }

                $dataLen = Protocol::unpack(Protocol::BIT_B32, substr($this->readBuffer, 0, 4));
                $this->readNeedLength = $dataLen;
                $this->readBuffer = substr($this->readBuffer, 4);
            }

            if (strlen($this->readBuffer) < $this->readNeedLength) {
                return;
            }

            $data = (string)substr($this->readBuffer, 0, $this->readNeedLength);

            $this->readBuffer = substr($this->readBuffer, $this->readNeedLength);
            $this->readNeedLength = 0;

            ($this->onReadable)($data, (int)$this->stream);
        } while (strlen($this->readBuffer));
    }

    /**
     * @param string|null $data
     */
    public function write(?string $data = null): void
    {
        if ($data !== null) {
            $this->writeBuffer .= $data;
        }

        do {
            $bytesToWrite = strlen($this->writeBuffer);
            $bytesWritten = @fwrite($this->stream, $this->writeBuffer);

            if ($this->isSocketDead()) {
                $this->reconnect();
            }
            $this->writeBuffer = substr($this->writeBuffer, $bytesWritten);
        } while (is_string($this->writeBuffer) && strlen($this->writeBuffer));
    }

    /**
     * check the stream is close
     */
    protected function isSocketDead(): bool
    {
        return !is_resource($this->stream) || @feof($this->stream);
    }
}
