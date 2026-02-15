-- Alter script to fix unique constraint on tc_final_output
-- Change from (run_id, tender_id, lot_number, supplier_rut) to (run_id, tender_id, lot_number, supplier_name)

-- Drop the old unique constraint if exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'tc_final_output_run_id_tender_id_lot_number_supplier_rut_key'
    ) THEN
        ALTER TABLE tc_final_output DROP CONSTRAINT tc_final_output_run_id_tender_id_lot_number_supplier_rut_key;
    END IF;
END $$;

-- Drop the index if exists
DROP INDEX IF EXISTS idx_tc_final_output_unique;

-- Create new unique constraint with supplier_name
CREATE UNIQUE INDEX idx_tc_final_output_unique 
ON tc_final_output (run_id, tender_id, lot_number, supplier_name);
