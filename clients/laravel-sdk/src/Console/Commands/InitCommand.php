<?php

declare(strict_types=1);

namespace FlowCatalyst\Console\Commands;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Cookie\CookieJar;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Console\Command;

/**
 * Scaffold a new application on a local fc-dev instance.
 *
 * Mirrors the TypeScript SDK's `flowcatalyst init` bin command:
 *
 *   1. Localhost guard — refuses non-localhost base URLs without
 *      --allow-remote.
 *   2. Logs in as anchor admin (fc-dev default credentials are
 *      admin@flowcatalyst.local / DevPassword123!) and captures
 *      the `fc_session` cookie for follow-up requests.
 *   3. Resolves (or creates) a "Default Client" so the application
 *      is properly client-scoped in FlowCatalyst's multi-client model.
 *   4. Creates the Application with the prompted code/name/type.
 *   5. Provisions a service account on it — the platform's
 *      `provision-service-account` endpoint mints a CONFIDENTIAL OAuth
 *      client with `client_credentials` grant in the same transaction
 *      and returns the plaintext `clientSecret` exactly once.
 *   6. Writes FLOWCATALYST_BASE_URL / _APP_CODE / _CLIENT_ID /
 *      _CLIENT_SECRET to ./.env (update-in-place for existing keys,
 *      append-with-comment for new ones).
 *
 * The init is one-shot bootstrap; for ongoing definition sync use
 * `flowcatalyst:scan` + `flowcatalyst:sync`.
 */
class InitCommand extends Command
{
    /** @var array<string, true> */
    private const LOCAL_HOSTS = [
        'localhost' => true,
        '127.0.0.1' => true,
        '::1' => true,
        '0.0.0.0' => true,
    ];

    private const DEFAULT_BASE_URL = 'http://localhost:8080';
    private const DEFAULT_ADMIN_EMAIL = 'admin@flowcatalyst.local';
    private const DEFAULT_ADMIN_PASSWORD = 'DevPassword123!';
    private const DEFAULT_CLIENT_IDENTIFIER = 'default';
    private const DEFAULT_CLIENT_NAME = 'Default Client';

    protected $signature = 'flowcatalyst:init
                            {--base-url= : Platform base URL (env FLOWCATALYST_BASE_URL, default http://localhost:8080)}
                            {--allow-remote : Permit non-localhost base URLs}
                            {--email= : Anchor admin email (default admin@flowcatalyst.local)}
                            {--password= : Password (prompts if omitted)}
                            {--code= : Application code (URL-safe slug)}
                            {--name= : Application name}
                            {--type= : APPLICATION or INTEGRATION}
                            {--description= : Optional description}
                            {--default-base-url= : Application deployed base URL}
                            {--client-identifier=default : Default client identifier}
                            {--client-name= : Default client name (default "Default Client")}
                            {--yes : Non-interactive — fail if any required value is missing}';

    protected $description = 'Scaffold a new application on a local fc-dev: app + service account + OAuth client + write credentials to .env';

    public function handle(): int
    {
        $baseUrl = (string) ($this->option('base-url')
            ?: env('FLOWCATALYST_BASE_URL', self::DEFAULT_BASE_URL));
        $parsed = parse_url($baseUrl);
        $host = $parsed['host'] ?? '';

        // 1. Localhost guard.
        if (!isset(self::LOCAL_HOSTS[$host]) && !$this->option('allow-remote')) {
            $this->error("flowcatalyst:init refuses to run against {$baseUrl} —");
            $this->error("hostname \"{$host}\" is not local. Pass --allow-remote to override");
            $this->error('if you really want to scaffold against this environment.');
            return self::INVALID;
        }

        $yes = (bool) $this->option('yes');
        $this->line("flowcatalyst:init → {$baseUrl}");
        $this->newLine();

        // 2. Gather inputs (CLI flag > prompt > default).
        $email = $this->resolve('email', 'Admin email', self::DEFAULT_ADMIN_EMAIL, $yes);
        $password = $this->resolveSecret('password', 'Admin password', $yes);
        $code = $this->resolve('code', 'Application code (slug, e.g. "orders")', null, $yes);
        $name = $this->resolve('name', 'Application name (e.g. "Orders")', null, $yes);
        $type = strtoupper($this->resolveChoice(
            'type',
            'Application type',
            ['APPLICATION', 'INTEGRATION'],
            'APPLICATION',
            $yes
        ));
        $description = (string) $this->resolve('description', 'Description (optional)', '', $yes);
        $defaultBaseUrl = (string) $this->resolve(
            'default-base-url',
            "Application's deployed base URL (optional)",
            '',
            $yes
        );
        $clientIdentifier = (string) $this->option('client-identifier') ?: self::DEFAULT_CLIENT_IDENTIFIER;
        $clientName = (string) ($this->option('client-name') ?: self::DEFAULT_CLIENT_NAME);

        // 3. Login.
        $jar = new CookieJar();
        $http = $this->makeHttpClient($baseUrl, $jar);

        $this->line('→ Logging in as ' . $email . '...');
        try {
            $this->request($http, 'POST', '/auth/login', [
                'email' => $email,
                'password' => $password,
            ]);
        } catch (\Throwable $e) {
            $this->error('Login failed: ' . $e->getMessage());
            return self::FAILURE;
        }
        $this->line('  ok');

        // 4. Resolve (or create) the Default Client.
        $this->line('→ Resolving default client "' . $clientIdentifier . '"...');
        $clientId = $this->ensureClient($http, $clientIdentifier, $clientName);

        // 5. Create the Application.
        $this->line('→ Creating application "' . $code . '"...');
        $appPayload = [
            'code' => $code,
            'name' => $name,
            'type' => $type,
        ];
        if ($description !== '') {
            $appPayload['description'] = $description;
        }
        if ($defaultBaseUrl !== '') {
            $appPayload['defaultBaseUrl'] = $defaultBaseUrl;
        }
        try {
            $appResp = $this->request($http, 'POST', '/api/applications', $appPayload);
        } catch (\Throwable $e) {
            $this->error('Create application failed: ' . $e->getMessage());
            return self::FAILURE;
        }
        $appId = $appResp['id'] ?? null;
        if (!is_string($appId)) {
            $this->error('Create application: missing "id" in response.');
            return self::FAILURE;
        }
        $this->line("  created app id={$appId}");

        // 6. Provision SA + OAuth client (PR #6 flow — same PG tx, returns
        // plaintext clientSecret exactly once).
        $this->line('→ Provisioning service account + OAuth client...');
        try {
            $provision = $this->request(
                $http,
                'POST',
                "/api/applications/{$appId}/provision-service-account",
                []
            );
        } catch (\Throwable $e) {
            $this->error('Provision service account failed: ' . $e->getMessage());
            return self::FAILURE;
        }
        $sa = $provision['serviceAccount'] ?? null;
        $oauth = is_array($sa) ? ($sa['oauthClient'] ?? null) : null;
        $publicClientId = is_array($oauth) ? ($oauth['clientId'] ?? null) : null;
        $secret = is_array($oauth) ? ($oauth['clientSecret'] ?? null) : null;
        if (!is_array($sa) || !is_string($publicClientId) || !is_string($secret)) {
            $this->error('Provision service account: unexpected response shape.');
            $this->line(json_encode($provision, JSON_PRETTY_PRINT) ?: '');
            return self::FAILURE;
        }
        $principalId = $sa['principalId'] ?? '(unknown)';
        $this->line("  ok — principal={$principalId}");

        // 7. Write .env.
        $envPath = base_path('.env');
        $updates = [
            'FLOWCATALYST_BASE_URL' => $baseUrl,
            'FLOWCATALYST_APP_CODE' => $code,
            'FLOWCATALYST_CLIENT_ID' => $publicClientId,
            'FLOWCATALYST_CLIENT_SECRET' => $secret,
        ];
        $this->writeEnv($envPath, $updates, $yes);

        $this->newLine();
        $this->info('✓ Application scaffolded.');
        $this->newLine();
        $this->line("  Application:     {$name} (code={$code})");
        $this->line("  Service account: {$principalId}");
        $this->line("  OAuth client:    {$publicClientId}");
        $this->line("  Default client:  {$clientId}");
        $this->newLine();
        $this->line('  Credentials written to .env. The clientSecret is shown ONLY in');
        $this->line('  the .env — the platform stores only the encrypted form and cannot');
        $this->line('  return it again. Rotate via the OAuth Clients page if needed.');

        return self::SUCCESS;
    }

    /**
     * Resolve a value: CLI flag → interactive prompt → default → error if --yes.
     */
    private function resolve(string $flag, string $question, ?string $default, bool $yes): string
    {
        $value = $this->option($flag);
        if (is_string($value) && $value !== '') {
            return $value;
        }
        if ($yes) {
            if ($default !== null) {
                return $default;
            }
            $this->error("--{$flag} required (in --yes mode every value must come from a flag).");
            exit(self::INVALID);
        }
        $prompted = $this->ask($question, $default);
        return is_string($prompted) ? $prompted : ($default ?? '');
    }

    private function resolveSecret(string $flag, string $question, bool $yes): string
    {
        $value = $this->option($flag);
        if (is_string($value) && $value !== '') {
            return $value;
        }
        if ($yes) {
            $this->error("--{$flag} required (in --yes mode every value must come from a flag).");
            exit(self::INVALID);
        }
        $this->line(sprintf('  (fc-dev default password is "%s")', self::DEFAULT_ADMIN_PASSWORD));
        $secret = $this->secret($question);
        return is_string($secret) ? $secret : '';
    }

    /**
     * @param list<string> $options
     */
    private function resolveChoice(
        string $flag,
        string $question,
        array $options,
        string $default,
        bool $yes
    ): string {
        $value = $this->option($flag);
        if (is_string($value) && $value !== '') {
            return $value;
        }
        if ($yes) {
            return $default;
        }
        $chosen = $this->choice($question, $options, array_search($default, $options, true) ?: 0);
        return is_string($chosen) ? $chosen : $default;
    }

    private function makeHttpClient(string $baseUrl, CookieJar $jar): GuzzleClient
    {
        return new GuzzleClient([
            'base_uri' => rtrim($baseUrl, '/') . '/',
            'cookies' => $jar,
            'http_errors' => false,
            'timeout' => 30,
            'headers' => [
                'Accept' => 'application/json',
                'Content-Type' => 'application/json',
            ],
        ]);
    }

    /**
     * @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private function request(GuzzleClient $http, string $method, string $path, array $body): array
    {
        try {
            $response = $http->request($method, ltrim($path, '/'), [
                'body' => $method === 'GET' || empty($body) && $method !== 'POST'
                    ? null
                    : json_encode($body, JSON_THROW_ON_ERROR),
            ]);
        } catch (GuzzleException $e) {
            throw new \RuntimeException("{$method} {$path}: " . $e->getMessage(), 0, $e);
        }

        $status = $response->getStatusCode();
        $contents = (string) $response->getBody();

        if ($status < 200 || $status >= 300) {
            throw new \RuntimeException(
                "{$method} {$path} → {$status} " . $response->getReasonPhrase()
                . ($contents !== '' ? "\n  {$contents}" : '')
            );
        }

        if ($status === 204 || $contents === '') {
            return [];
        }

        $decoded = json_decode($contents, true);
        if (!is_array($decoded)) {
            return [];
        }
        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    private function ensureClient(GuzzleClient $http, string $identifier, string $name): string
    {
        try {
            $existing = $this->request(
                $http,
                'GET',
                '/api/clients/by-identifier/' . rawurlencode($identifier),
                []
            );
            if (isset($existing['id']) && is_string($existing['id'])) {
                $this->line("  reusing existing client id={$existing['id']}");
                return $existing['id'];
            }
        } catch (\RuntimeException $e) {
            // 404 → fall through to create. Anything else is fatal.
            if (!str_contains($e->getMessage(), '404')) {
                throw $e;
            }
        }

        $created = $this->request($http, 'POST', '/api/clients', [
            'identifier' => $identifier,
            'name' => $name,
        ]);
        if (!isset($created['id']) || !is_string($created['id'])) {
            throw new \RuntimeException('Create client: missing "id" in response.');
        }
        $this->line("  created client id={$created['id']}");
        return $created['id'];
    }

    /**
     * @param array<string, string> $updates
     */
    private function writeEnv(string $path, array $updates, bool $yes): void
    {
        $original = is_file($path) ? (string) file_get_contents($path) : '';
        $lines = $original === '' ? [] : preg_split('/\r?\n/', $original);
        $lines = $lines === false ? [] : $lines;
        $seen = [];

        foreach ($lines as $i => $line) {
            if (preg_match('/^\s*([A-Z_][A-Z0-9_]*)\s*=/', $line, $m) === 1) {
                $key = $m[1];
                if (isset($updates[$key])) {
                    $lines[$i] = $key . '=' . $this->quoteEnvValue($updates[$key]);
                    $seen[$key] = true;
                }
            }
        }

        $toAppend = array_diff_key($updates, $seen);
        if (!empty($toAppend)) {
            if (!empty($lines) && end($lines) !== '') {
                $lines[] = '';
            }
            $lines[] = '# FlowCatalyst (added by `php artisan flowcatalyst:init`)';
            foreach ($toAppend as $k => $v) {
                $lines[] = $k . '=' . $this->quoteEnvValue($v);
            }
        }

        $next = rtrim(implode("\n", $lines), "\n") . "\n";
        if ($next === $original) {
            $this->line('→ .env already has these values, no update needed.');
            return;
        }

        if ($original !== '' && !$yes && !$this->confirm('Update ' . $path . '?', true)) {
            $this->warn('✗ skipped writing .env; credentials below:');
            foreach ($updates as $k => $v) {
                $this->line('    ' . $k . '=' . $this->quoteEnvValue($v));
            }
            return;
        }

        file_put_contents($path, $next);
        $this->line('→ .env ' . ($original === '' ? 'created' : 'updated') . '.');
    }

    private function quoteEnvValue(string $value): string
    {
        if ($value === '' || preg_match('/[\s#\'"`$]/', $value) === 1) {
            return "'" . str_replace("'", "'\\''", $value) . "'";
        }
        return $value;
    }
}
