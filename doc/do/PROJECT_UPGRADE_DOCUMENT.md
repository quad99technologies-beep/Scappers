# Pharma Scraper Platform - Enterprise Upgrade Document

**Version**: 2.0  
**Date**: February 2026  
**Status**: Production Ready  
**Classification**: Internal Use

---

## Executive Summary

This document outlines the comprehensive upgrade of the Pharma Scraper Platform from a single-node scraping system to an enterprise-grade, distributed, AI-powered data intelligence platform.

### Current State (v1.0)
- Single-node execution with file-based locks
- 4 production scrapers (Malaysia, India, Argentina, Russia)
- Basic Telegram notifications
- SQLite/PostgreSQL dual support
- Manual VPN management

### Target State (v2.0)
- Distributed 5-Mac architecture with Celery+Redis
- 10+ country scrapers with auto-healing
- AI-powered extraction and schema inference
- Built-in proxy pool with geo-routing
- 500+ integrations via n8n
- Complete observability stack

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Hardware Specifications](#2-hardware-specifications)
3. [Core Feature Upgrades](#3-core-feature-upgrades)
4. [New High-Value Features](#4-new-high-value-features)
5. [Implementation Phases](#5-implementation-phases)
6. [Pre-Migration Checklist](#6-pre-migration-checklist)
7. [Post-Migration Checklist](#7-post-migration-checklist)
8. [Risk Assessment](#8-risk-assessment)
9. [Rollback Procedures](#9-rollback-procedures)
10. [Appendices](#10-appendices)

---

## 1. Architecture Overview

### 1.1 Current Architecture (v1.0)

```
┌─────────────────────────────────────────┐
│         Single Windows/Mac Machine      │
│  ┌─────────────────────────────────┐    │
│  │  Scraper GUI (Tkinter)          │    │
│  │  - Manual scraper execution     │    │
│  │  - Basic progress tracking      │    │
│  └─────────────────────────────────┘    │
│                   │                     │
│  ┌─────────────────────────────────┐    │
│  │  Scraper Scripts (Sequential)   │    │
│  │  - Malaysia (Playwright)        │    │
│  │  - India (Scrapy)               │    │
│  │  - Argentina (Selenium+VPN)     │    │
│  │  - Russia (Selenium)            │    │
│  └─────────────────────────────────┘    │
│                   │                     │
│  ┌─────────────────────────────────┐    │
│  │  Database (SQLite/PostgreSQL)   │    │
│  │  - Local file storage           │    │
│  └─────────────────────────────────┘    │
│                   │                     │
│  ┌─────────────────────────────────┐    │
│  │  Telegram Bot (Basic)           │    │
│  │  - Start/stop notifications     │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘

Limitations:
- Single concurrent scraper (max_concurrent_runs: 1)
- Manual VPN switching
- No automatic failover
- Limited observability
- No distributed processing
```

### 1.2 Target Architecture (v2.0)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DISTRIBUTED ARCHITECTURE v2.0                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Mac Studio M1/M2/M3 (128GB)                      │   │
│  │                         "AI BRAIN"                                  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │   │
│  │  │   Ollama    │  │  ChromaDB   │  │  Embeddings │  │  MLflow   │  │   │
│  │  │  LLM Server │  │ Vector Store│  │    API      │  │ (optional)│  │   │
│  │  │  :11434     │  │   :8000     │  │   :8080     │  │           │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘  │   │
│  │                                                                     │   │
│  │  Models: llama3.3:70b, qwen2.5:72b, llava:34b, nomic-embed-text    │   │
│  │  Functions: Extraction, Classification, Healing, Summarization      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ▲                                        │
│                                    │ HTTP/REST                               │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐   │
│  │                    Mac Mini M4 (32GB) - "DATABASE NODE"               │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐    │   │
│  │  │ PostgreSQL  │  │    Redis    │  │    MinIO    │  │  Vault    │    │   │
│  │  │  Primary DB │  │Task Queue   │  │S3-Compatible│  │  Secrets  │    │   │
│  │  │  :5432      │  │   :6379     │  │  :9000      │  │  :8200    │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘    │   │
│  │                                                                     │   │
│  │  Storage: 2TB Thunderbolt SSD (DB + Backups)                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ▲                                        │
│                                    │                                        │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐   │
│  │                    Mac Mini M4 (32GB) - "CONTROLLER NODE"             │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐    │   │
│  │  │  FastAPI    │  │    n8n      │  │   Grafana   │  │  Traefik  │    │   │
│  │  │   Gateway   │  │  Workflow   │  │ Dashboards  │  │  Proxy    │    │   │
│  │  │   :8000     │  │  :5678      │  │   :3000     │  │  :80/443  │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘    │   │
│  │                                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │   │
│  │  │ Prometheus  │  │ Telegram Bot│  │Celery Beat  │                   │   │
│  │  │  :9090      │  │             │  │Scheduler    │                   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│              ┌─────────────────────┴─────────────────────┐                  │
│              │                                           │                  │
│  ┌───────────▼───────────┐                 ┌─────────────▼────────────┐    │
│  │ Mac Mini M2 (16GB)    │                 │ Mac Mini M2 (16GB)       │    │
│  │    "WORKER-1"         │                 │    "WORKER-2"            │    │
│  │  ┌─────────────────┐  │                 │  ┌─────────────────┐     │    │
│  │  │ Celery Worker   │  │                 │  │ Celery Worker   │     │    │
│  │  │   x2 instances  │  │                 │  │   x2 instances  │     │    │
│  │  └─────────────────┘  │                 │  └─────────────────┘     │    │
│  │  ┌─────────────────┐  │                 │  ┌─────────────────┐     │    │
│  │  │ Chrome/Playwright│  │                 │  │ Chrome/Playwright│    │    │
│  │  │   (isolated)    │  │                 │  │   (isolated)    │     │    │
│  │  └─────────────────┘  │                 │  └─────────────────┘     │    │
│  │  ┌─────────────────┐  │                 │  ┌─────────────────┐     │    │
│  │  │  VPN Client     │  │                 │  │  VPN Client     │     │    │
│  │  │  (Country A)    │  │                 │  │  (Country B)    │     │    │
│  │  └─────────────────┘  │                 │  └─────────────────┘     │    │
│  │  ┌─────────────────┐  │                 │  ┌─────────────────┐     │    │
│  │  │  Proxy Pool     │  │                 │  │  Proxy Pool     │     │    │
│  │  │  (Rotating)     │  │                 │  │  (Rotating)     │     │    │
│  │  └─────────────────┘  │                 │  └─────────────────┘     │    │
│  └───────────────────────┘                 └──────────────────────────┘    │
│                                                                             │
│  Network: 10GbE/2.5GbE Switch                                               │
│  Storage: Synology NAS (4TB+) for backups and shared storage                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Key Improvements:
- 4 concurrent scrapers (was 1)
- AI-powered extraction and healing
- Built-in proxy pool with geo-routing
- 500+ integrations via n8n
- Complete observability (metrics, logs, traces)
- Automatic failover and health checks
- Zero cloud dependencies
```

---

## 2. Hardware Specifications

### 2.1 Recommended Hardware Configuration

| Machine | Role | Specs | Est. Cost | Qty | Total |
|---------|------|-------|-----------|-----|-------|
| Mac Studio M2 Ultra | AI/LLM Server | 24-core CPU, 60-core GPU, 128GB RAM, 1TB SSD | $4,000 | 1 | $4,000 |
| Mac Mini M4 Pro | Database Node | 14-core CPU, 20-core GPU, 32GB RAM, 512GB SSD | $1,600 | 1 | $1,600 |
| Mac Mini M4 Pro | Controller Node | 14-core CPU, 20-core GPU, 32GB RAM, 512GB SSD | $1,600 | 1 | $1,600 |
| Mac Mini M2 Pro | Worker Node | 12-core CPU, 19-core GPU, 16GB RAM, 256GB SSD | $800 | 2 | $1,600 |
| Synology DS423+ | NAS/Storage | 4-bay, 8TB drives (RAID 5) | $1,200 | 1 | $1,200 |
| 10GbE Switch | Networking | 8-port 10GbE | $300 | 1 | $300 |
| UPS (1500VA) | Power | APC/ CyberPower | $200 | 3 | $600 |
| **TOTAL** | | | | **8** | **$10,900** |

*Alternative: Use existing Mac Studio M1 128GB instead of M2 Ultra - save $4,000*

### 2.2 Alternative: Budget Configuration

| Machine | Role | Specs | Est. Cost |
|---------|------|-------|-----------|
| Mac Studio M1 (existing) | AI/LLM | 128GB RAM | $0 |
| Mac Mini M4 (32GB) | Database | 32GB RAM | $1,600 |
| Mac Mini M4 (32GB) | Controller | 32GB RAM | $1,600 |
| Mac Mini M2 (16GB) x2 | Workers | 16GB RAM each | $1,200 |
| Synology DS220j | NAS | 2-bay, 4TB | $400 |
| **TOTAL** | | | **$4,800** |

---

## 3. Core Feature Upgrades

### 3.1 Distributed Task Queue (Celery + Redis)

**Purpose**: Enable horizontal scaling across multiple worker nodes

**Implementation**:
```python
# core/distributed.py
from celery import Celery

app = Celery('scraper_platform', broker='redis://mac-mini-db:6379/0')

@app.task(bind=True, max_retries=3)
def scrape_country(self, country: str, config: dict):
    """Distributed scraper task"""
    runner = WorkflowRunner(country, config)
    return runner.run()

# Schedule periodic tasks
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(hour=2), scrape_country.s('Malaysia'))
    sender.add_periodic_task(crontab(hour=4), scrape_country.s('Argentina'))
```

**Benefits**:
- 4x throughput (4 workers vs 1)
- Automatic retry with exponential backoff
- Task prioritization
- Dead letter queues for failed tasks

### 3.2 AI-Powered Features (Ollama on Mac Studio)

**Purpose**: Reduce manual maintenance, improve extraction accuracy

**Features**:
1. **Auto Schema Inference**: LLM analyzes HTML and suggests selectors
2. **Dynamic Extraction**: Extract data without predefined schemas
3. **Auto-Healing**: Fix broken selectors when sites change
4. **Content Classification**: Identify pharmaceutical products vs other content
5. **CAPTCHA Solving**: Vision models for image-based challenges
6. **Natural Language Queries**: "Show me drugs under $50" → SQL

**Models**:
- `llama3.3:70b` - High-quality extraction (40GB)
- `qwen2.5:72b` - Code/SQL generation (45GB)
- `llava:34b` - Vision/CAPTCHA solving (20GB)
- `llama3.2:3b` - Fast inference (2GB)
- `nomic-embed-text` - Embeddings (500MB)

### 3.3 Built-in Proxy Pool Manager

**Purpose**: Stability and scale beyond VPN limitations

**Features**:
- Health checking with automatic failover
- Geo-targeting by country (MY, AR, RU, IN, etc.)
- Session persistence (sticky sessions)
- Rate limiting per proxy
- Support for datacenter, residential, and mobile proxies
- Integration with Bright Data, Oxylabs, Smartproxy

**Usage**:
```python
from core.proxy_pool import ProxyPool, ProxyType

pool = ProxyPool()

# Get proxy for specific country
proxy = pool.get_proxy(
    country_code="MY",
    proxy_type=ProxyType.RESIDENTIAL
)

# Use in requests
response = requests.get(url, proxies=proxy.dict_format)

# Report result
pool.report_success(proxy.id, response_time_ms=500)
```

### 3.4 One-Click Geo Routing

**Purpose**: Automatically map scrapers to appropriate IPs

**Implementation**:
```python
# core/geo_router.py
class GeoRouter:
    """Automatic geo-routing for scrapers"""
    
    COUNTRY_ROUTES = {
        "Malaysia": {"country_code": "MY", "vpn": "singapore", "proxy_type": "residential"},
        "Argentina": {"country_code": "AR", "vpn": "argentina", "proxy_type": "residential"},
        "India": {"country_code": "IN", "vpn": "india", "proxy_type": "isp"},
        "Russia": {"country_code": "RU", "vpn": "russia", "proxy_type": "datacenter"},
    }
    
    def get_route(self, scraper_name: str) -> dict:
        """Get routing configuration for scraper"""
        return self.COUNTRY_ROUTES.get(scraper_name, {})
    
    def apply_route(self, scraper_name: str, driver_or_session):
        """Automatically apply VPN and proxy settings"""
        route = self.get_route(scraper_name)
        
        # Connect VPN if needed
        if route.get("vpn"):
            self.vpn_manager.connect(route["vpn"])
        
        # Get proxy if needed
        if route.get("proxy_type"):
            proxy = self.proxy_pool.get_proxy(
                country_code=route["country_code"],
                proxy_type=route["proxy_type"]
            )
            return proxy
```

### 3.5 Crawl Frontier Queue

**Purpose**: Queue product/detail pages discovered during scrape

**Implementation**:
```python
# core/frontier.py
from urllib.parse import urljoin, urlparse
import hashlib

class CrawlFrontier:
    """Lightweight crawl frontier for discovered URLs"""
    
    def __init__(self, redis_client, scraper_name: str):
        self.redis = redis_client
        self.scraper = scraper_name
        self.queue_key = f"frontier:{scraper_name}:queue"
        self.seen_key = f"frontier:{scraper_name}:seen"
    
    def add_url(self, url: str, priority: int = 0, metadata: dict = None):
        """Add URL to frontier"""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        
        # Skip if already seen
        if self.redis.sismember(self.seen_key, url_hash):
            return False
        
        # Add to queue with priority (sorted set)
        self.redis.zadd(self.queue_key, {json.dumps({
            "url": url,
            "metadata": metadata or {},
            "added_at": datetime.utcnow().isoformat()
        }): priority})
        
        # Mark as seen
        self.redis.sadd(self.seen_key, url_hash)
        return True
    
    def get_next(self, count: int = 1) -> list:
        """Get next URLs to crawl"""
        urls = []
        for _ in range(count):
            item = self.redis.zpopmax(self.queue_key, 1)
            if item:
                data = json.loads(item[0][0])
                urls.append(data)
        return urls
    
    def mark_completed(self, url: str, success: bool):
        """Mark URL as completed"""
        status_key = f"frontier:{self.scraper}:completed"
        self.redis.hset(status_key, url, json.dumps({
            "success": success,
            "completed_at": datetime.utcnow().isoformat()
        }))
```

### 3.6 n8n Workflow Automation

**Purpose**: 500+ integrations without coding

**Key Workflows**:
1. **Scraper Completion** → Slack + Email + Google Sheets
2. **Data Quality Alert** → PagerDuty + Dashboard
3. **Price Change Detected** → CRM Update + Alert
4. **Daily Summary** → Executive Report
5. **Failed Scraper** → Auto-retry + Escalation

**Integration Points**:
- CRM: HubSpot, Salesforce, Zoho
- Storage: Google Drive, Dropbox, S3
- Communication: Slack, Teams, Discord, WhatsApp
- Databases: MySQL, MongoDB, Airtable
- AI: OpenAI, Anthropic, Local LLM

---

## 4. New High-Value Features

### 4.1 Visual Crawl Explorer UI (Optional)

**Purpose**: Debug and audit crawls visually

**Features**:
- Screenshot capture per step
- DOM tree visualization
- Network request logging
- Replay crawl sessions
- Compare before/after site changes

**Implementation**: Web-based UI on Controller Node

### 4.2 Auto Site Discovery Crawler (Optional)

**Purpose**: Discover new pages within known domains

**Features**:
- Sitemap parsing
- Link extraction and filtering
- URL pattern learning
- Duplicate detection
- Respect robots.txt

**Note**: Limited to configured domains, not broad web crawling

### 4.3 Enhanced Observability

**Metrics** (Prometheus):
- scraper_runs_total (by country, status)
- scraper_duration_seconds (histogram)
- items_scraped_total (counter)
- proxy_success_rate (gauge)
- llm_requests_total (counter)
- data_quality_score (gauge)

**Tracing** (OpenTelemetry):
- Distributed traces across workers
- Pipeline step timing
- Database query performance
- LLM call latency

**Logging** (Loki/Grafana):
- Centralized log aggregation
- Structured JSON logging
- Log correlation by run_id
- Alert on error patterns

---

## 5. Implementation Phases

### Phase 0: Preparation (Week 0)

**Goals**: Document current state, backup everything

**Tasks**:
- [ ] Export all current database data
- [ ] Document current VPN configurations
- [ ] List all API keys and credentials
- [ ] Screenshot current GUI workflows
- [ ] Note current cron jobs/scheduled tasks
- [ ] Inventory existing hardware

**Deliverables**:
- Complete database backup
- Credential inventory
- Current state documentation

---

### Phase 1: Infrastructure (Weeks 1-2)

**Goals**: Deploy core infrastructure services

**Week 1: Database & Queue**
- [ ] Set up Mac Mini M4 (Database Node)
- [ ] Install Docker and Docker Compose
- [ ] Deploy PostgreSQL with optimizations
- [ ] Deploy Redis for task queue
- [ ] Deploy MinIO for object storage
- [ ] Configure backups to NAS
- [ ] Test connectivity from all nodes

**Week 2: Controller & Monitoring**
- [ ] Set up Mac Mini M4 (Controller Node)
- [ ] Deploy FastAPI gateway
- [ ] Deploy Prometheus and Grafana
- [ ] Deploy Traefik reverse proxy
- [ ] Deploy Telegram Bot
- [ ] Configure SSL certificates
- [ ] Set up basic monitoring dashboards

**Deliverables**:
- Working database cluster
- Redis queue operational
- Monitoring accessible
- API responding

---

### Phase 2: AI Infrastructure (Weeks 3-4)

**Goals**: Deploy LLM capabilities

**Week 3: LLM Server**
- [ ] Set up Mac Studio (AI Node)
- [ ] Install Ollama
- [ ] Download core models (3B, 7B, 70B)
- [ ] Test inference speed
- [ ] Configure model caching
- [ ] Set up ChromaDB for embeddings

**Week 4: AI Integration**
- [ ] Create LLM extractor module
- [ ] Implement schema inference
- [ ] Build auto-healing system
- [ ] Test CAPTCHA solving
- [ ] Benchmark against manual extraction

**Deliverables**:
- LLM server responding
- Extraction accuracy >90%
- Auto-healing functional

---

### Phase 3: Workers & Distribution (Weeks 5-6)

**Goals**: Deploy distributed workers

**Week 5: Worker Setup**
- [ ] Set up Mac Mini M2 #1 (Worker)
- [ ] Set up Mac Mini M2 #2 (Worker)
- [ ] Install Docker on both
- [ ] Deploy Celery workers
- [ ] Configure Chrome/Playwright
- [ ] Set up VPN clients
- [ ] Test task distribution

**Week 6: Proxy Pool**
- [ ] Deploy proxy pool manager
- [ ] Add initial proxy list
- [ ] Configure geo-routing
- [ ] Test health checking
- [ ] Integrate with scrapers
- [ ] Benchmark vs VPN-only

**Deliverables**:
- 4 concurrent workers operational
- Proxy pool with 10+ proxies
- Geo-routing functional

---

### Phase 4: Integration & Automation (Weeks 7-8)

**Goals**: Connect everything

**Week 7: n8n & Workflows**
- [ ] Deploy n8n on Controller
- [ ] Create notification workflows
- [ ] Set up Google Sheets integration
- [ ] Configure Slack/Teams alerts
- [ ] Test webhook triggers
- [ ] Document workflow creation

**Week 8: Migration & Testing**
- [ ] Migrate Malaysia scraper
- [ ] Migrate India scraper
- [ ] Migrate Argentina scraper
- [ ] Migrate Russia scraper
- [ ] Run parallel (old vs new)
- [ ] Validate data consistency

**Deliverables**:
- All scrapers on new platform
- n8n workflows operational
- Data validation passed

---

### Phase 5: Optimization (Weeks 9-10)

**Goals**: Performance tuning and polish

**Week 9: Performance**
- [ ] Tune PostgreSQL queries
- [ ] Optimize Celery task size
- [ ] Adjust proxy rotation
- [ ] Cache frequently accessed data
- [ ] Benchmark full pipeline
- [ ] Document performance baseline

**Week 10: Documentation & Training**
- [ ] Write operations manual
- [ ] Create troubleshooting guide
- [ ] Train team on new system
- [ ] Document disaster recovery
- [ ] Create runbooks
- [ ] Archive old system docs

**Deliverables**:
- Performance benchmarks
- Complete documentation
- Team trained

---

### Phase 6: Advanced Features (Weeks 11-12)

**Goals**: Optional enhancements

- [ ] Deploy visual crawl explorer
- [ ] Implement crawl frontier
- [ ] Add more countries
- [ ] Fine-tune custom LLM models
- [ ] Implement advanced analytics

**Deliverables**:
- Visual explorer UI
- Frontier queue operational
- 2+ additional countries

---

## 6. Pre-Migration Checklist

### 6.1 Data Backup

- [ ] Export all PostgreSQL databases
- [ ] Export all SQLite databases
- [ ] Backup configuration files
- [ ] Export Telegram bot data
- [ ] Archive old run logs
- [ ] Document current schema versions

### 6.2 Hardware Preparation

- [ ] Unbox and inventory all new hardware
- [ ] Update macOS on all machines
- [ ] Install Xcode Command Line Tools
- [ ] Install Homebrew
- [ ] Configure static IPs or DHCP reservations
- [ ] Test network connectivity between nodes
- [ ] Label all machines with roles

### 6.3 Software Prerequisites

- [ ] Install Docker Desktop on all Macs
- [ ] Install Docker Compose
- [ ] Configure Docker resources (CPU/RAM)
- [ ] Install Python 3.11+
- [ ] Install Git
- [ ] Clone repository to all nodes
- [ ] Set up SSH keys for node communication

### 6.4 Credential Inventory

- [ ] List all API keys (Telegram, OpenAI, etc.)
- [ ] Document VPN credentials
- [ ] List proxy provider accounts
- [ ] Document database passwords
- [ ] List external service credentials
- [ ] Prepare Vault/secret management

### 6.5 Network Configuration

- [ ] Configure router port forwarding (if needed)
- [ ] Set up local DNS or hosts file entries
- [ ] Test inter-node connectivity
- [ ] Configure firewall rules
- [ ] Set up VPN server (if self-hosted)
- [ ] Test external access to controller

### 6.6 Documentation Review

- [ ] Review current scraper documentation
- [ ] Document known issues
- [ ] List custom modifications
- [ ] Note hardcoded paths/IPs
- [ ] Document scheduled tasks
- [ ] Archive old documentation

---

## 7. Post-Migration Checklist

### 7.1 Verification Tests

- [ ] All services start without errors
- [ ] Database connections successful
- [ ] Redis queue responding
- [ ] LLM inference working
- [ ] Proxy pool healthy
- [ ] Celery workers registered
- [ ] API responding correctly
- [ ] Grafana dashboards loading
- [ ] Telegram bot responding

### 7.2 Scraper Validation

- [ ] Malaysia scraper completes successfully
- [ ] India scraper completes successfully
- [ ] Argentina scraper completes successfully
- [ ] Russia scraper completes successfully
- [ ] Data quality checks pass
- [ ] Export files generated correctly
- [ ] Notifications sent successfully
- [ ] Run logs captured properly

### 7.3 Performance Validation

- [ ] 4 concurrent scrapers run without conflict
- [ ] Database query times <100ms
- [ ] LLM response time acceptable
- [ ] Proxy rotation working
- [ ] Memory usage stable
- [ ] No Chrome process leaks
- [ ] Network throughput acceptable

### 7.4 Integration Validation

- [ ] n8n workflows trigger correctly
- [ ] Slack notifications received
- [ ] Email alerts delivered
- [ ] Google Sheets updated
- [ ] Webhooks fire correctly
- [ ] CRM sync working (if configured)

### 7.5 Monitoring Setup

- [ ] All metrics flowing to Prometheus
- [ ] Grafana dashboards configured
- [ ] Alert rules defined
- [ ] Alert channels tested
- [ ] Log aggregation working
- [ ] Health checks passing

### 7.6 Documentation Updates

- [ ] Update architecture diagrams
- [ ] Document new procedures
- [ ] Update troubleshooting guide
- [ ] Document known limitations
- [ ] Create quick reference card
- [ ] Archive old documentation

### 7.7 Team Handover

- [ ] Train team on new system
- [ ] Demonstrate key workflows
- [ ] Provide access credentials
- [ ] Share documentation links
- [ ] Schedule follow-up training
- [ ] Establish support channels

---

## 8. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data loss during migration | Low | Critical | Multiple backups, parallel run |
| Hardware failure | Low | High | Warranty coverage, spare parts |
| Network issues | Medium | Medium | Test thoroughly, fallback configs |
| Scraper breakages | Medium | Medium | Parallel testing, quick rollback |
| LLM performance issues | Medium | Medium | Fallback to smaller models |
| Team adoption resistance | Medium | Medium | Training, documentation |
| Budget overrun | Low | Medium | Phased approach, track costs |

---

## 9. Rollback Procedures

### 9.1 Quick Rollback (if issues detected)

```bash
# 1. Stop new services
docker-compose -f docker-compose.new.yml down

# 2. Start old services
cd /path/to/old/installation
python scraper_gui.py

# 3. Verify old system works
# Run test scraper
```

### 9.2 Full Rollback (if major issues)

1. **Stop all new services**
   ```bash
   # On all nodes
   docker-compose down
   docker system prune -a
   ```

2. **Restore databases**
   ```bash
   # Restore from pre-migration backup
   pg_restore -d scraper_platform backup_pre_migration.sql
   ```

3. **Restore configuration**
   ```bash
   # Copy backed up config files
   cp -r backup/config/* config/
   ```

4. **Restart old system**
   ```bash
   python scraper_gui.py
   ```

5. **Verify functionality**
   - Run test scrapes
   - Check data integrity
   - Verify notifications

---

## 10. Appendices

### Appendix A: Docker Compose Files

See separate files:
- `docker-compose.db.yml` - Database services
- `docker-compose.controller.yml` - Controller services
- `docker-compose.worker.yml` - Worker services
- `docker-compose.llm.yml` - LLM services

### Appendix B: Environment Variables

```bash
# Database
DATABASE_URL=postgresql://scraper:password@mac-mini-db:5432/scraper_platform
REDIS_URL=redis://mac-mini-db:6379/0

# LLM
OLLAMA_URL=http://mac-studio:11434

# Storage
MINIO_ENDPOINT=mac-mini-db:9000
MINIO_ACCESS_KEY=scraper
MINIO_SECRET_KEY=password

# Secrets
VAULT_ADDR=http://mac-mini-db:8200
VAULT_TOKEN=token

# Notifications
TELEGRAM_BOT_TOKEN=token
SLACK_WEBHOOK_URL=url

# Proxy
PROXY_POOL_ENABLED=true
BRIGHTDATA_API_KEY=key
```

### Appendix C: Network Diagram

```
Internet
    │
    ▼
Router (Port forwarding: 80, 443, 5678, 3000)
    │
    ▼
10GbE Switch
    │
    ├── Mac Studio (192.168.1.10) - LLM
    ├── Mac Mini M4 (192.168.1.11) - DB
    ├── Mac Mini M4 (192.168.1.12) - Controller
    ├── Mac Mini M2 (192.168.1.13) - Worker 1
    ├── Mac Mini M2 (192.168.1.14) - Worker 2
    └── Synology NAS (192.168.1.20) - Storage
```

### Appendix D: Support Contacts

| Role | Name | Contact |
|------|------|---------|
| Project Lead | | |
| DevOps | | |
| Database Admin | | |
| Hardware Vendor | Apple | support.apple.com |
| NAS Vendor | Synology | synology.com |

---

## Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-03 | | Initial document |

---

**END OF DOCUMENT**
