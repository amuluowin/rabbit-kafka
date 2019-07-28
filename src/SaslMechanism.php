<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\socket\socket\SocketClientInterface;

interface SaslMechanism
{
    /**
     *
     * sasl authenticate
     *
     * @access public
     */
    public function authenticate(SocketClientInterface $socket): void;
}
