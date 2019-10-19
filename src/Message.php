<?php
/**
 * Created by PhpStorm.
 * User: albert
 * Date: 18-3-29
 * Time: 下午8:43
 */

namespace rabbit\kafka;

class Message
{
    /**
     * @var string[]
     */
    private $message;

    /**
     * @return string[]
     */
    public function getMessage(): array
    {
        return $this->message ?: [];
    }

    /**
     * @param string[] $message
     */
    public function setMessage(array $message): void
    {
        $this->message = $message;
    }
}
