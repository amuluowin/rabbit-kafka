<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\kafka\Producter\Producter;

/**
 * Class ProductModel
 * @package rabbit\kafka
 */
class ProductModel extends Model
{
    /** @var Producter */
    protected $producter;

    /**
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function save(bool $validated = true): void
    {
        $validated && $this->validate();
        $this->producter->send([
            [
                'topic' => $this->topic,
                'value' => json_encode($this->attributes),
                'key' => $this->key
            ]
        ]);
    }
}