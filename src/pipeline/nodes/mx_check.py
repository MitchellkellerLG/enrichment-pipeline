"""
MX Record Check Node

Validates that a domain has valid MX records, indicating the domain
can receive email. This is a free check using DNS lookups.
"""

import socket
from typing import Optional
from urllib.parse import urlparse


def extract_domain(website: str) -> Optional[str]:
    """
    Extract the domain from a website URL.

    Handles various formats:
    - https://example.com
    - http://www.example.com/path
    - example.com
    - www.example.com

    Args:
        website: The website URL or domain string

    Returns:
        The extracted domain, or None if invalid
    """
    if not website:
        return None

    website = website.strip()

    # Add scheme if missing for proper parsing
    if not website.startswith(('http://', 'https://')):
        website = 'https://' + website

    try:
        parsed = urlparse(website)
        domain = parsed.netloc or parsed.path.split('/')[0]

        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]

        # Remove port if present
        if ':' in domain:
            domain = domain.split(':')[0]

        # Basic validation - must have at least one dot
        if '.' not in domain or len(domain) < 4:
            return None

        return domain.lower()
    except Exception:
        return None


def run_mx_check(company_name: str, website: str) -> dict:
    """
    Check if a domain has valid MX records.

    Performs a DNS MX record lookup to verify the domain can receive email.
    This is a free check with no API costs.

    Args:
        company_name: The company name (unused, for consistency with other nodes)
        website: The website URL or domain to check

    Returns:
        dict with keys:
            - mx_valid: bool - True if MX records were found
            - mx_records: list[str] - List of MX hostnames found
            - domain: str - The domain that was checked
            - cost_usd: float - Always 0 (free check)
            - error: str or None - Error message if check failed

    Example:
        >>> result = run_mx_check("Example Corp", "https://example.com")
        >>> result["mx_valid"]
        True
        >>> result["mx_records"]
        ['mail.example.com']
    """
    result = {
        "mx_valid": False,
        "mx_records": [],
        "domain": None,
        "cost_usd": 0,
        "error": None
    }

    # Extract domain from website
    domain = extract_domain(website)
    if not domain:
        result["error"] = f"Invalid domain format: {website}"
        return result

    result["domain"] = domain

    # Set socket timeout for DNS operations
    original_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(5.0)

    try:
        # Try to use dnspython if available (more reliable)
        try:
            import dns.resolver

            resolver = dns.resolver.Resolver()
            resolver.timeout = 5
            resolver.lifetime = 5

            try:
                mx_records = resolver.resolve(domain, 'MX')
                mx_hosts = [str(mx.exchange).rstrip('.') for mx in mx_records]

                if mx_hosts:
                    result["mx_valid"] = True
                    result["mx_records"] = sorted(mx_hosts)
                else:
                    result["error"] = "No MX records found"

            except dns.resolver.NXDOMAIN:
                result["error"] = f"Domain does not exist: {domain}"
            except dns.resolver.NoAnswer:
                result["error"] = "No MX records configured for domain"
            except dns.resolver.Timeout:
                result["error"] = "DNS lookup timed out"
            except dns.resolver.NoNameservers:
                result["error"] = "No nameservers available for domain"
            except Exception as e:
                result["error"] = f"DNS lookup failed: {str(e)}"

        except ImportError:
            # Fallback to socket-based MX lookup
            # This is less reliable but works without dnspython
            import subprocess
            import sys

            # Use nslookup on Windows, dig on Unix
            if sys.platform == 'win32':
                try:
                    proc = subprocess.run(
                        ['nslookup', '-type=mx', domain],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    output = proc.stdout

                    # Parse nslookup output for MX records
                    mx_hosts = []
                    for line in output.split('\n'):
                        if 'mail exchanger' in line.lower():
                            # Extract hostname from line like "MX preference = 10, mail exchanger = mail.example.com"
                            parts = line.split('=')
                            if len(parts) >= 3:
                                mx_host = parts[-1].strip().rstrip('.')
                                if mx_host:
                                    mx_hosts.append(mx_host)

                    if mx_hosts:
                        result["mx_valid"] = True
                        result["mx_records"] = sorted(set(mx_hosts))
                    else:
                        result["error"] = "No MX records found"

                except subprocess.TimeoutExpired:
                    result["error"] = "DNS lookup timed out"
                except FileNotFoundError:
                    result["error"] = "nslookup not available"
                except Exception as e:
                    result["error"] = f"MX lookup failed: {str(e)}"
            else:
                # Unix - try dig
                try:
                    proc = subprocess.run(
                        ['dig', '+short', 'MX', domain],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    output = proc.stdout.strip()

                    if output:
                        mx_hosts = []
                        for line in output.split('\n'):
                            parts = line.split()
                            if len(parts) >= 2:
                                mx_host = parts[1].rstrip('.')
                                mx_hosts.append(mx_host)

                        if mx_hosts:
                            result["mx_valid"] = True
                            result["mx_records"] = sorted(mx_hosts)
                        else:
                            result["error"] = "No MX records found"
                    else:
                        result["error"] = "No MX records found"

                except subprocess.TimeoutExpired:
                    result["error"] = "DNS lookup timed out"
                except FileNotFoundError:
                    result["error"] = "dig not available"
                except Exception as e:
                    result["error"] = f"MX lookup failed: {str(e)}"

    finally:
        # Restore original socket timeout
        socket.setdefaulttimeout(original_timeout)

    return result


if __name__ == "__main__":
    # Test the function
    test_cases = [
        ("Google", "https://google.com"),
        ("Microsoft", "www.microsoft.com"),
        ("Example", "example.com"),
        ("Invalid", "not-a-real-domain-xyz123.com"),
        ("Bad Format", ""),
        ("With Path", "https://github.com/user/repo"),
    ]

    for company, website in test_cases:
        print(f"\nTesting: {company} - {website}")
        result = run_mx_check(company, website)
        print(f"  Domain: {result['domain']}")
        print(f"  Valid: {result['mx_valid']}")
        print(f"  Records: {result['mx_records']}")
        print(f"  Error: {result['error']}")
