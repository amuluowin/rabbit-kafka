<?php
declare(strict_types=1);

namespace rabbit\kafka\Sasl;

use GSSAPIContext;
use KRB5CCache;
use rabbit\kafka\CommonSocket;
use rabbit\kafka\Exception;
use rabbit\kafka\Exception\NotSupported;
use rabbit\kafka\Protocol\Protocol as ProtocolTool;
use function extension_loaded;
use function file_exists;
use function is_file;
use function is_readable;
use const GSS_C_INITIATE;

class Gssapi extends Mechanism
{
    private const MECHANISM_NAME = 'GSSAPI';
    /**
     * @var KRB5CCache
     */
    private static $ccache;
    /**
     * @var string
     */
    private $principal;
    /**
     * @var GSSAPIContext
     */
    private $gssapi;

    public function __construct(GSSAPIContext $gssapi, string $principal)
    {
        $this->gssapi = $gssapi;
        $this->principal = $principal;
    }

    public static function fromKeytab(string $keytab, string $principal): self
    {
        if (!extension_loaded('krb5')) {
            throw new Exception('Extension "krb5" is required for "GSSAPI" authentication');
        }

        if (!file_exists($keytab) || !is_file($keytab)) {
            throw new Exception('Invalid keytab, keytab file not exists.');
        }

        if (!is_readable($keytab)) {
            throw new Exception('Invalid keytab, keytab file disable read.');
        }

        self::$ccache = new KRB5CCache();
        self::$ccache->initKeytab($principal, $keytab);

        $gssapi = new GSSAPIContext();
        $gssapi->acquireCredentials(self::$ccache, $principal, GSS_C_INITIATE);
        return new self($gssapi, $principal);
    }

    /**
     *
     * get sasl authenticate mechanism name
     *
     * @access public
     */
    public function getName(): string
    {
        return self::MECHANISM_NAME;
    }

    /**
     * @throws NotSupported
     * @throws Exception
     */
    protected function performAuthentication(CommonSocket $socket): void
    {
        $token = $this->initSecurityContext();

        // send token to server and get server token
        $data = ProtocolTool::encodeString($token, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);

        $dataLen = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->readBlocking(4));

        // wrap message use server token and send to server authenticate
        $data = ProtocolTool::encodeString(
            $this->wrapToken($socket->readBlocking($dataLen)),
            ProtocolTool::PACK_INT32
        );

        $socket->writeBlocking($data);
    }

    private function initSecurityContext(): string
    {
        $token = '';
        $ret = $this->gssapi->initSecContext($this->principal, null, null, null, $token);
        if (!$ret) {
            throw new Exception('Init security context failure.');
        }
        return $token;
    }

    private function wrapToken(string $token): string
    {
        $message = '';
        $this->gssapi->wrap($token, $message);
        return $message;
    }
}
