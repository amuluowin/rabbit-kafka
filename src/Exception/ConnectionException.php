<?php
declare(strict_types=1);

namespace rabbit\kafka\Exception;

use rabbit\kafka\Exception;
use function sprintf;

/**
 * Class ConnectionException
 * @package rabbit\kafka\Exception
 */
final class ConnectionException extends Exception
{
    /**
     * @param string $brokerList
     * @return ConnectionException
     */
    public static function fromBrokerList(string $brokerList): self
    {
        return new self(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }
}
