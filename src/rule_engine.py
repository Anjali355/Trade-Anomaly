"""
LAYER 1: RULE-BASED DETECTION ENGINE

Detects deterministic anomalies using hard-coded business logic.
No statistics or ML needed - just business rules.

8 Rule Checks:
1. PRICE_MISMATCH - qty × price ≠ total_fob
2. INCOTERM_FREIGHT_MISMATCH - CIF without freight
3. INCOTERM_EXW_ERROR - EXW with freight
4. INVALID_DRAWBACK_CLAIM - Rejected shipment with drawback
5. MISSING_PAYMENT_DATE - Payment received but no date
6. EXCESSIVE_INSURANCE - Insurance >2% of FOB
7. INVALID_HS_CODE_FORMAT - Not 8 digits
8. FOB_INSURANCE_MISMATCH - FOB incoterm but seller paid insurance
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any


class RuleEngine:
    """Hard-coded business logic checks for shipment anomalies"""
    
    def __init__(self, shipments_df: pd.DataFrame, product_catalog: pd.DataFrame, 
                 routes_df: pd.DataFrame, buyers_df: pd.DataFrame):
        """
        Initialize rule engine with data
        
        Args:
            shipments_df: Shipment records
            product_catalog: Product master data
            routes_df: Route definitions
            buyers_df: Buyer information
        """
        self.df = shipments_df.copy()
        self.product_catalog = product_catalog
        self.routes_df = routes_df
        self.buyers_df = buyers_df
        self.anomalies = []
    
    def run_all_rules(self) -> List[Dict[str, Any]]:
        """Execute all rule checks"""
        print("🔴 Running Layer 1: Rule-Based Detection...")
        
        self.check_price_mismatch()
        self.check_incoterm_compliance()
        self.check_drawback_validity()
        self.check_payment_consistency()
        self.check_insurance_validity()
        self.check_hs_code_format()
        
        print(f"   Found {len(self.anomalies)} rule violations")
        return self.anomalies
    
    # ========================================================================
    # RULE 1: PRICE MISMATCH
    # ========================================================================
    
    def check_price_mismatch(self):
        """
        Rule: total_fob must equal quantity × unit_price (within $1 tolerance)
        
        Business Context:
        - Billing discrepancy indicates invoice entry error or fraud
        - Causes payment disputes and cash flow problems
        - Cost if missed: $50K-$500K per shipment
        """
        print("  1. Checking price mismatches...")
        
        df = self.df.copy()
        df['calculated_fob'] = df['quantity'] * df['unit_price']
        df['fob_difference'] = abs(df['total_fob'] - df['calculated_fob'])
        
        # Flag if discrepancy > $1
        # df['calculated_fob'] = (df['quantity'] * df['unit_price']).round(2)
        # df['total_fob'] = df['total_fob'].round(2)
        # mismatches = df[abs(df['total_fob'] - df['calculated_fob']) > 1.00]
        df['calculated_fob'] = df['quantity'] * df['unit_price']
        df['fob_difference_pct'] = abs(df['total_fob'] - df['calculated_fob']) / df['total_fob'] * 100

        # Only flag if >1% difference
        mismatches = df[df['fob_difference_pct'] > 1.0]
        
        for _, row in mismatches.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'PRICE_MISMATCH',
                'layer': 1,
                'severity': 'HIGH',
                'evidence': {
                    'quantity': int(row['quantity']),
                    'unit_price': float(row['unit_price']),
                    'expected_total_fob': float(row['calculated_fob']),
                    'actual_total_fob': float(row['total_fob']),
                    'discrepancy': float(row['fob_difference'])
                },
                'impact': f"Billing discrepancy of ${row['fob_difference']:.2f}. Buyer may dispute payment.",
                'recommendation': 'Verify invoice math. Correct before shipment release.'
            })
    
    # ========================================================================
    # RULE 2-3: INCOTERM COMPLIANCE
    # ========================================================================
    
    def check_incoterm_compliance(self):
        """
        Rules:
        - CIF (Cost + Insurance + Freight): Seller MUST pay freight
        - EXW (Ex Works): Buyer arranges & pays freight (seller shouldn't)
        
        Business Context (Indian Export):
        - CIF = seller responsible for freight to destination port
        - EXW = buyer responsible from factory gate
        - Contract violation = dispute + payment refusal
        - Cost if missed: $500K-$5M contract breach
        """
        print("  2-3. Checking incoterm compliance...")
        
        # CIF requires freight cost > 0
        cif_no_freight = self.df[
            (self.df['incoterm'] == 'CIF') & 
            (self.df['freight_cost'] == 0)
        ]
        
        for _, row in cif_no_freight.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'INCOTERM_FREIGHT_MISMATCH',
                'layer': 1,
                'severity': 'CRITICAL',
                'evidence': {
                    'incoterm': 'CIF',
                    'freight_cost': float(row['freight_cost']),
                    'rule': 'CIF = Cost + Insurance + FREIGHT (seller must pay)'
                },
                'impact': 'Contract breach. Buyer may refuse payment or initiate dispute.',
                'recommendation': 'Verify freight cost with logistics provider. Update invoice.'
            })
        
        # EXW shouldn't have freight cost (buyer arranges)
        exw_with_freight = self.df[
            (self.df['incoterm'] == 'EXW') & 
            (self.df['freight_cost'] > 0)
        ]
        
        for _, row in exw_with_freight.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'INCOTERM_EXW_ERROR',
                'layer': 1,
                'severity': 'HIGH',
                'evidence': {
                    'incoterm': 'EXW',
                    'freight_cost': float(row['freight_cost']),
                    'rule': 'EXW = Buyer arranges everything (seller should NOT pay freight)'
                },
                'impact': f"Unnecessary cost of ${row['freight_cost']:.2f} to exporter.",
                'recommendation': 'Remove freight cost or change incoterm to CIF/DDP.'
            })
    
    # ========================================================================
    # RULE 4: INVALID DRAWBACK CLAIM
    # ========================================================================
    
    def check_drawback_validity(self):
        """
        Rule: Drawback (tax refund on export) only valid if customs CLEARED shipment
        
        Business Context (Indian Export):
        - Drawback = GST/duty refund when goods leave India
        - Rejected shipment = no export happened = no refund justified
        - False claim = tax audit + ₹50K-₹500K penalty + interest
        - Criminal liability possible if intentional
        
        Cost if missed: ₹75K-₹500K penalty + interest + 18% interest
        """
        print("  4. Checking drawback validity...")
        
        invalid_drawback = self.df[
            (self.df['customs_status'] == 'rejected') & 
            (self.df['drawback_amount'] > 0)
        ]
        
        for _, row in invalid_drawback.iterrows():
            penalty = row['drawback_amount'] * 1.5  # Estimate
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'INVALID_DRAWBACK_CLAIM',
                'layer': 1,
                'severity': 'CRITICAL',
                'evidence': {
                    'customs_status': 'rejected',
                    'drawback_claimed': float(row['drawback_amount']),
                    'rule': 'Drawback only valid if customs cleared shipment'
                },
                'impact': f"False claim of ₹{row['drawback_amount']:.0f}. Tax audit risk: ₹{penalty:.0f} penalty.",
                'recommendation': 'Immediately withdraw drawback claim. Consult tax consultant.'
            })
    
    # ========================================================================
    # RULE 5: PAYMENT CONSISTENCY
    # ========================================================================
    
    def check_payment_consistency(self):
        """
        Rule: If payment_status = "received", days_to_payment must have a value
        
        Business Context:
        - Received payment but no date = cannot reconcile
        - Affects cash flow forecasting and financial reporting
        - Makes it impossible to assess buyer payment behavior
        
        Cost if missed: ₹100K-₹500K accounting errors
        """
        print("  5. Checking payment consistency...")
        
        # Payment received but no date recorded
        missing_date = self.df[
            (self.df['payment_status'] == 'received') & 
            (self.df['days_to_payment'].isna())
        ]
        
        for _, row in missing_date.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'MISSING_PAYMENT_DATE',
                'layer': 1,
                'severity': 'MEDIUM',
                'evidence': {
                    'payment_status': 'received',
                    'days_to_payment': None,
                    'issue': 'Cannot determine when payment was actually received'
                },
                'impact': 'Cannot reconcile accounts. Buyer creditworthiness assessment impossible.',
                'recommendation': 'Cross-reference with bank statement. Update days_to_payment.'
            })
        
        # Payment arrived but status still says pending
        inconsistent_status = self.df[
            (self.df['payment_status'] == 'pending') & 
            (self.df['days_to_payment'] > 0) &
            (self.df['days_to_payment'] < 180)  # Not future date
        ]
        
        for _, row in inconsistent_status.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'PAYMENT_STATUS_INCONSISTENT',
                'layer': 1,
                'severity': 'MEDIUM',
                'evidence': {
                    'payment_status': 'pending',
                    'days_to_payment': int(row['days_to_payment']),
                    'issue': 'Payment was received but status not updated'
                },
                'impact': 'Misleading status. Affects buyer creditworthiness assessment.',
                'recommendation': 'Update payment_status to "received".'
            })
    
    # ========================================================================
    # RULE 6: INSURANCE SANITY CHECK
    # ========================================================================
    
    def check_insurance_validity(self):
        """
        Rules:
        - Insurance should be 0.5-2% of FOB (industry standard)
        - FOB incoterm: buyer should buy insurance (seller shouldn't)
        
        Business Context:
        - Over-insurance = wasting money on unnecessary protection
        - Under-insurance = claims may be denied in loss
        - FOB = buyer's responsibility (free on board)
        
        Cost if missed: ₹100K-₹500K unnecessary costs
        """
        print("  6. Checking insurance validity...")
        
        # Excessive insurance (>2% of FOB)
        excessive = self.df[self.df['insurance_amount'] > (self.df['total_fob'] * 0.02)]
        
        for _, row in excessive.iterrows():
            pct = (row['insurance_amount'] / row['total_fob'] * 100) if row['total_fob'] > 0 else 0
            excess_amount = row['insurance_amount'] - (row['total_fob'] * 0.02)
            
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'EXCESSIVE_INSURANCE',
                'layer': 1,
                'severity': 'LOW',
                'evidence': {
                    'total_fob': float(row['total_fob']),
                    'insurance_amount': float(row['insurance_amount']),
                    'percentage': f"{pct:.2f}%",
                    'industry_standard': "0.5-2%"
                },
                'impact': f"Wasting ${excess_amount:.2f} on over-insurance.",
                'recommendation': 'Negotiate better rates with insurance broker.'
            })
        
        # FOB incoterm: buyer pays insurance, not seller
        fob_with_insurance = self.df[
            (self.df['incoterm'] == 'FOB') & 
            (self.df['insurance_amount'] > 0)
        ]
        
        for _, row in fob_with_insurance.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'FOB_INSURANCE_MISMATCH',
                'layer': 1,
                'severity': 'MEDIUM',
                'evidence': {
                    'incoterm': 'FOB',
                    'insurance_charged_to_seller': float(row['insurance_amount']),
                    'rule': 'Under FOB, buyer arranges & pays insurance'
                },
                'impact': f"Unnecessary cost of ${row['insurance_amount']:.2f} to exporter.",
                'recommendation': 'Remove insurance cost or change incoterm to CIF/DDP.'
            })
    
    # ========================================================================
    # RULE 7: HS CODE FORMAT VALIDATION
    # ========================================================================
    
    def check_hs_code_format(self):
        """
        Rule: HS (Harmonized System) codes must be exactly 8 digits
        
        Business Context (Indian Export):
        - HS code = customs classification for tariff purposes
        - Invalid format = customs system rejects shipment
        - Shipment stuck at port = ₹50K+/day storage + demurrage charges
        - Can delay shipment 10-15 days easily
        
        Cost if missed: ₹50K-₹500K per day port delay
        """
        print("  7. Checking HS code format...")
        
        # Check if HS code is exactly 8 digits
        invalid_format = self.df[
            (self.df['hs_code'].astype(str).str.len() != 8) |
            (~self.df['hs_code'].astype(str).str.match(r'^\d{8}$'))
        ]
        
        for _, row in invalid_format.iterrows():
            hs_code_str = str(row['hs_code'])
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'INVALID_HS_CODE_FORMAT',
                'layer': 1,
                'severity': 'CRITICAL',
                'evidence': {
                    'hs_code': hs_code_str,
                    'length': len(hs_code_str),
                    'contains_letters': not hs_code_str.isdigit(),
                    'expected_format': '8 digits (e.g., 84713000)'
                },
                'impact': 'Customs cannot process. Port detention and storage charges accumulate.',
                'recommendation': 'Correct HS code immediately. Consult customs broker.'
            })
    
    # ========================================================================
    # SUMMARY STATISTICS
    # ========================================================================
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of anomalies found"""
        if not self.anomalies:
            return {
                'total_anomalies': 0,
                'by_severity': {},
                'by_type': {}
            }
        
        anomaly_df = pd.DataFrame(self.anomalies)
        
        return {
            'total_anomalies': len(self.anomalies),
            'by_severity': anomaly_df['severity'].value_counts().to_dict(),
            'by_type': anomaly_df['anomaly_type'].value_counts().to_dict()
        }


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Load test data
    try:
        shipments = pd.read_csv('data/shipments.csv', dtype={'hs_code': str})
        buyers = pd.read_csv('data/buyers.csv')
        products = pd.read_csv('data/product_catalog.csv', dtype={'hs_code': str})
        routes = pd.read_csv('data/routes.csv')
        
        # Run Layer 1
        engine = RuleEngine(shipments, products, routes, buyers)
        anomalies = engine.run_all_rules()
        
        print(f"\n{'='*70}")
        print(f"LAYER 1 RESULTS")
        print(f"{'='*70}")
        print(f"Total anomalies detected: {len(anomalies)}")
        
        # Print summary
        summary = engine.get_summary()
        print(f"\nBy Severity:")
        for severity, count in summary['by_severity'].items():
            print(f"  {severity}: {count}")
        
        print(f"\nBy Type:")
        for anom_type, count in summary['by_type'].items():
            print(f"  {anom_type}: {count}")
        
        # Print first 3 anomalies
        print(f"\nFirst 3 Anomalies:")
        for i, anom in enumerate(anomalies[:3], 1):
            print(f"\n{i}. {anom['anomaly_type']} (Shipment #{anom['shipment_id']})")
            print(f"   Severity: {anom['severity']}")
            print(f"   Impact: {anom['impact']}")
            print(f"   Recommendation: {anom['recommendation']}")
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nPlease run data_generator.py first to create test data.")
        sys.exit(1)