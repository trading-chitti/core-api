"""Stock Configuration Routes - Manage stock settings for features and broker assignment."""

from typing import Optional, List
from datetime import datetime
import uuid
import csv
import io
import asyncio

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os

router = APIRouter()

PG_DSN = os.getenv('TRADING_CHITTI_PG_DSN', 'postgresql://hariprasath@localhost:6432/trading_chitti')

# ============================================================================
# Models
# ============================================================================

class StockConfigFilter(BaseModel):
    """Filter parameters for stock configuration list."""
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    intraday_enabled: Optional[bool] = None
    investment_enabled: Optional[bool] = None
    fetcher: Optional[str] = None
    active: Optional[bool] = True


class StockConfigUpdate(BaseModel):
    """Update stock configuration."""
    intraday_enabled: Optional[bool] = None
    investment_enabled: Optional[bool] = None
    fetcher: Optional[str] = None
    active: Optional[bool] = None


class BulkStockConfigUpdate(BaseModel):
    """Bulk update multiple stocks."""
    symbol_exchange_pairs: List[tuple[str, str]]  # [(symbol, exchange), ...]
    intraday_enabled: Optional[bool] = None
    investment_enabled: Optional[bool] = None
    fetcher: Optional[str] = None
    active: Optional[bool] = None


class CSVImportJobStatus(BaseModel):
    """CSV import job status."""
    job_id: str
    filename: str
    total_rows: int
    processed_rows: int
    successful_rows: int
    failed_rows: int
    status: str
    progress_percentage: float
    error_message: Optional[str] = None
    started_at: str
    completed_at: Optional[str] = None
    estimated_completion_at: Optional[str] = None


# ============================================================================
# Stock Configuration Endpoints
# ============================================================================

@router.get("/stocks")
async def list_stock_configs(
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    exchange: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    market_cap_category: Optional[str] = Query(None),
    intraday_enabled: Optional[bool] = Query(None),
    investment_enabled: Optional[bool] = Query(None),
    fetcher: Optional[str] = Query(None),
    active: Optional[bool] = Query(True),
    selection_type: Optional[str] = Query(None),
):
    """
    Get list of stock configurations with filtering and pagination.

    Query Parameters:
    - limit: Number of results per page (default 100, max 1000)
    - offset: Pagination offset
    - symbol: Filter by symbol (partial match)
    - name: Filter by name (partial match; also matches symbol)
    - exchange: Filter by exchange (NSE, BSE, US)
    - sector: Filter by sector (partial match)
    - market_cap_category: Filter by market cap category (Small, Mid, Large, nil)
    - intraday_enabled: Filter by intraday feature status
    - investment_enabled: Filter by investment feature status
    - fetcher: Filter by assigned broker (ZERODHA, INDMONEY, PAYTM)
    - active: Filter by active status (default: true)
    - selection_type: Filter by AI selection type (MORNING_ML, WILDCARD_NEWS, NOT_SELECTED)
    """
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build WHERE clause
        where_clauses = []
        params = []

        if active is not None:
            where_clauses.append("sc.active = %s")
            params.append(active)

        if symbol:
            where_clauses.append("sc.symbol ILIKE %s")
            params.append(f"%{symbol}%")

        if name:
            normalized_name = name.strip()
            if normalized_name:
                name_like = f"%{normalized_name}%"
                where_clauses.append("(sc.name ILIKE %s OR sc.symbol ILIKE %s)")
                params.extend([name_like, name_like])

        if exchange:
            where_clauses.append("sc.exchange = %s")
            params.append(exchange)

        if sector:
            where_clauses.append("sc.sector ILIKE %s")
            params.append(f"%{sector}%")

        if market_cap_category:
            where_clauses.append("sc.market_cap_category = %s")
            params.append(market_cap_category)

        if intraday_enabled is not None:
            where_clauses.append("sc.intraday_enabled = %s")
            params.append(intraday_enabled)

        if investment_enabled is not None:
            where_clauses.append("sc.investment_enabled = %s")
            params.append(investment_enabled)

        if fetcher:
            where_clauses.append("sc.fetcher = %s")
            params.append(fetcher)

        if selection_type:
            if selection_type == 'NOT_SELECTED':
                where_clauses.append("(sc.intraday_ai_picked = FALSE OR sc.intraday_ai_picked IS NULL)")
            elif selection_type in ('MORNING_ML', 'WILDCARD_NEWS'):
                where_clauses.append("sc.selection_type = %s")
                params.append(selection_type)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM md.stock_config sc WHERE {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()['total']

        # Get paginated results
        query = f"""
            SELECT
                sc.id, sc.symbol, sc.exchange, sc.name, sc.sector,
                sc.intraday_enabled, sc.investment_enabled, sc.fetcher,
                sc.active, sc.created_at, sc.updated_at, sc.market_cap_category,
                sc.intraday_ai_picked, sc.selection_type
            FROM md.stock_config sc
            WHERE {where_clause}
            ORDER BY sc.symbol ASC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        cur.execute(query, params)
        stocks = cur.fetchall()

        # Convert to dict and format timestamps
        stocks_list = []
        for stock in stocks:
            stock_dict = dict(stock)
            if stock_dict.get('created_at'):
                stock_dict['created_at'] = stock_dict['created_at'].isoformat()
            if stock_dict.get('updated_at'):
                stock_dict['updated_at'] = stock_dict['updated_at'].isoformat()
            stocks_list.append(stock_dict)

        cur.close()
        conn.close()

        return {
            "stocks": stocks_list,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + len(stocks_list)) < total
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock configurations: {str(e)}")


@router.put("/stocks/{symbol}/{exchange}")
async def update_stock_config(
    symbol: str,
    exchange: str,
    config: StockConfigUpdate
):
    """Update configuration for a specific stock."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build UPDATE statement
        updates = []
        params = []

        if config.intraday_enabled is not None:
            updates.append("intraday_enabled = %s")
            params.append(config.intraday_enabled)

        if config.investment_enabled is not None:
            updates.append("investment_enabled = %s")
            params.append(config.investment_enabled)

        if config.fetcher is not None:
            updates.append("fetcher = %s")
            params.append(config.fetcher)

        if config.active is not None:
            updates.append("active = %s")
            params.append(config.active)

        if not updates:
            return {"message": "No updates provided"}

        params.extend([symbol, exchange])

        update_query = f"""
            UPDATE md.stock_config
            SET {", ".join(updates)}
            WHERE symbol = %s AND exchange = %s
            RETURNING id, symbol, exchange, name, intraday_enabled, investment_enabled, fetcher, active
        """

        cur.execute(update_query, params)
        updated_stock = cur.fetchone()

        if not updated_stock:
            conn.rollback()
            raise HTTPException(status_code=404, detail=f"Stock {symbol} ({exchange}) not found")

        conn.commit()
        cur.close()
        conn.close()

        return {
            "message": "Stock configuration updated successfully",
            "stock": dict(updated_stock)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update stock configuration: {str(e)}")


# ============================================================================
# CSV Export/Import
# ============================================================================

@router.get("/export-csv")
async def export_stock_configs_csv(
    active: Optional[bool] = Query(True),
    exchange: Optional[str] = Query(None),
    fetcher: Optional[str] = Query(None),
):
    """
    Export stock configurations as CSV file.

    Query Parameters:
    - active: Filter by active status (default: true)
    - exchange: Filter by exchange (NSE, BSE, US)
    - fetcher: Filter by broker (ZERODHA, INDMONEY, PAYTM)
    """
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build WHERE clause
        where_clauses = []
        params = []

        if active is not None:
            where_clauses.append("active = %s")
            params.append(active)

        if exchange:
            where_clauses.append("exchange = %s")
            params.append(exchange)

        if fetcher:
            where_clauses.append("fetcher = %s")
            params.append(fetcher)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        query = f"""
            SELECT
                symbol, exchange, name, sector,
                intraday_enabled, investment_enabled, fetcher, active, market_cap_category
            FROM md.stock_config
            WHERE {where_clause}
            ORDER BY symbol ASC
        """

        cur.execute(query, params)
        stocks = cur.fetchall()

        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'symbol', 'exchange', 'name', 'sector',
            'intraday_enabled', 'investment_enabled', 'fetcher', 'active', 'market_cap_category'
        ])
        writer.writeheader()
        writer.writerows([dict(stock) for stock in stocks])

        cur.close()
        conn.close()

        # Return CSV as streaming response
        csv_content = output.getvalue()
        output.close()

        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=stock_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export CSV: {str(e)}")


async def process_csv_import(job_id: str, csv_content: str):
    """Background task to process CSV import."""
    conn = None
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        # Update job status to processing
        cur.execute("""
            UPDATE md.csv_import_jobs
            SET status = 'processing'
            WHERE job_id = %s
        """, (job_id,))
        conn.commit()

        # Parse CSV
        csv_file = io.StringIO(csv_content)
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        total_rows = len(rows)

        # Update total rows
        cur.execute("""
            UPDATE md.csv_import_jobs
            SET total_rows = %s
            WHERE job_id = %s
        """, (total_rows, job_id))
        conn.commit()

        processed = 0
        successful = 0
        failed = 0

        start_time = datetime.now()

        for row in rows:
            try:
                symbol = row.get('symbol', '').strip()
                exchange = row.get('exchange', 'NSE').strip()

                if not symbol:
                    failed += 1
                    continue

                # Parse boolean fields
                intraday_enabled = row.get('intraday_enabled', 'false').lower() in ('true', '1', 'yes')
                investment_enabled = row.get('investment_enabled', 'false').lower() in ('true', '1', 'yes')
                active = row.get('active', 'true').lower() in ('true', '1', 'yes')

                fetcher = row.get('fetcher', '').strip().upper() or None
                if fetcher and fetcher not in ('ZERODHA', 'INDMONEY', 'PAYTM'):
                    fetcher = None

                # Update stock config
                cur.execute("""
                    UPDATE md.stock_config
                    SET intraday_enabled = %s,
                        investment_enabled = %s,
                        fetcher = %s,
                        active = %s
                    WHERE symbol = %s AND exchange = %s
                """, (intraday_enabled, investment_enabled, fetcher, active, symbol, exchange))

                if cur.rowcount > 0:
                    successful += 1
                else:
                    failed += 1

                processed += 1

                # Update progress every 100 rows
                if processed % 100 == 0:
                    progress = (processed / total_rows) * 100
                    elapsed = (datetime.now() - start_time).total_seconds()
                    estimated_total = (elapsed / processed) * total_rows if processed > 0 else 0
                    estimated_completion = start_time.timestamp() + estimated_total

                    cur.execute("""
                        UPDATE md.csv_import_jobs
                        SET processed_rows = %s,
                            successful_rows = %s,
                            failed_rows = %s,
                            progress_percentage = %s,
                            estimated_completion_at = to_timestamp(%s)
                        WHERE job_id = %s
                    """, (processed, successful, failed, progress, estimated_completion, job_id))
                    conn.commit()

            except Exception as e:
                failed += 1
                print(f"Error processing row {processed}: {e}")

        # Final update
        cur.execute("""
            UPDATE md.csv_import_jobs
            SET processed_rows = %s,
                successful_rows = %s,
                failed_rows = %s,
                progress_percentage = 100.0,
                status = 'completed',
                completed_at = NOW()
            WHERE job_id = %s
        """, (processed, successful, failed, job_id))
        conn.commit()

        cur.close()
        conn.close()

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE md.csv_import_jobs
                    SET status = 'failed',
                        error_message = %s,
                        completed_at = NOW()
                    WHERE job_id = %s
                """, (str(e), job_id))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass


@router.post("/import-csv")
async def import_stock_configs_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Import stock configurations from CSV file.
    Processing happens in the background. Use /import-jobs/{job_id} to check progress.

    CSV Format:
    symbol,exchange,name,sector,intraday_enabled,investment_enabled,fetcher,active,market_cap_category
    RELIANCE,NSE,Reliance Industries,Energy,true,true,ZERODHA,true,Large
    """
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")

        # Read CSV content
        csv_content = (await file.read()).decode('utf-8')

        # Create import job
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        job_id = str(uuid.uuid4())

        cur.execute("""
            INSERT INTO md.csv_import_jobs (job_id, filename, status)
            VALUES (%s, %s, 'pending')
            RETURNING job_id, filename, status, started_at
        """, (job_id, file.filename))

        job = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        # Schedule background task
        background_tasks.add_task(process_csv_import, job_id, csv_content)

        return {
            "message": "CSV import started in background",
            "job_id": job_id,
            "filename": file.filename,
            "status": "pending"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start CSV import: {str(e)}")


@router.get("/import-jobs/{job_id}")
async def get_import_job_status(job_id: str):
    """Get status of a CSV import job."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                job_id, filename, total_rows, processed_rows,
                successful_rows, failed_rows, status,
                progress_percentage, error_message,
                started_at, completed_at, estimated_completion_at
            FROM md.csv_import_jobs
            WHERE job_id = %s
        """, (job_id,))

        job = cur.fetchone()

        if not job:
            raise HTTPException(status_code=404, detail="Import job not found")

        job_dict = dict(job)

        # Format timestamps
        if job_dict.get('started_at'):
            job_dict['started_at'] = job_dict['started_at'].isoformat()
        if job_dict.get('completed_at'):
            job_dict['completed_at'] = job_dict['completed_at'].isoformat()
        if job_dict.get('estimated_completion_at'):
            job_dict['estimated_completion_at'] = job_dict['estimated_completion_at'].isoformat()

        cur.close()
        conn.close()

        return job_dict

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get import job status: {str(e)}")


@router.get("/import-jobs")
async def list_import_jobs(
    limit: int = Query(20, le=100),
    status: Optional[str] = Query(None)
):
    """List recent CSV import jobs."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        where_clause = "status = %s" if status else "1=1"
        params = [status] if status else []

        query = f"""
            SELECT
                job_id, filename, total_rows, processed_rows,
                successful_rows, failed_rows, status,
                progress_percentage, error_message,
                started_at, completed_at
            FROM md.csv_import_jobs
            WHERE {where_clause}
            ORDER BY started_at DESC
            LIMIT %s
        """
        params.append(limit)

        cur.execute(query, params)
        jobs = cur.fetchall()

        jobs_list = []
        for job in jobs:
            job_dict = dict(job)
            if job_dict.get('started_at'):
                job_dict['started_at'] = job_dict['started_at'].isoformat()
            if job_dict.get('completed_at'):
                job_dict['completed_at'] = job_dict['completed_at'].isoformat()
            jobs_list.append(job_dict)

        cur.close()
        conn.close()

        return {
            "jobs": jobs_list,
            "total": len(jobs_list)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list import jobs: {str(e)}")


# ============================================================================
# Statistics
# ============================================================================

@router.get("/stats")
async def get_stock_config_stats():
    """Get statistics about stock configurations."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get overall stats
        cur.execute("""
            SELECT
                COUNT(*) as total_stocks,
                COUNT(*) FILTER (WHERE active = true) as active_stocks,
                COUNT(*) FILTER (WHERE intraday_enabled = true) as intraday_enabled_count,
                COUNT(*) FILTER (WHERE investment_enabled = true) as investment_enabled_count,
                COUNT(*) FILTER (WHERE fetcher = 'ZERODHA') as zerodha_count,
                COUNT(*) FILTER (WHERE fetcher = 'INDMONEY') as indmoney_count,
                COUNT(*) FILTER (WHERE fetcher = 'PAYTM') as paytm_count,
                COUNT(*) FILTER (WHERE fetcher IS NULL) as unassigned_count
            FROM md.stock_config
        """)

        stats = dict(cur.fetchone())

        cur.close()
        conn.close()

        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")
