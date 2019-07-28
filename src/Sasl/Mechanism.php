<?php
declare(strict_types=1);

namespace rabbit\kafka\Sasl;

use rabbit\kafka\Exception;
use rabbit\kafka\Protocol;
use rabbit\kafka\Protocol\Protocol as ProtocolTool;
use rabbit\kafka\SaslMechanism;
use rabbit\socket\socket\SocketClientInterface;
use function substr;

abstract class Mechanism implements SaslMechanism
{
    public function authenticate(SocketClientInterface $socket): void
    {
        $this->handShake($socket, $this->getName());
        $this->performAuthentication($socket);
    }

    /**
     *
     * sasl authenticate hand shake
     *
     * @access protected
     */
    protected function handShake(SocketClientInterface $socket, string $mechanism): void
    {
        $requestData = Protocol::encode(Protocol::SASL_HAND_SHAKE_REQUEST, [$mechanism]);
        $socket->send($requestData);
        $dataLen = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->recv(4));

        $data = $socket->recv($dataLen);
        $correlationId = ProtocolTool::unpack(ProtocolTool::BIT_B32, substr($data, 0, 4));
        $result = Protocol::decode(Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 4));

        if ($result['errorCode'] !== Protocol::NO_ERROR) {
            throw new Exception(Protocol::getError($result['errorCode']));
        }
    }

    abstract public function getName(): string;

    abstract protected function performAuthentication(CommonSocket $socket): void;
}
