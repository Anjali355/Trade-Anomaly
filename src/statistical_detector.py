import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple


class StatisticalDetector:
    """Statistical anomaly detection using IQR method"""
    
    def __init__(self, shipments_df: pd.DataFrame, product_catalog: pd.DataFrame,
                 routes_df: pd.DataFrame, buyers_df: pd.DataFrame):
        """
        Initialize statistical detector
        
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
    
    def run_all_detections(self) -> List[Dict[str, Any]]:
        """Execute all statistical checks"""
        print("🟡 Running Layer 2: Statistical Detection (IQR)...")
        
        self.detect_price_outliers()
        self.detect_transit_time_outliers()
        self.detect_freight_outliers()
        self.detect_payment_behavior_change()
        self.detect_volume_spikes()
        
        print(f"   Found {len(self.anomalies)} statistical anomalies")
        return self.anomalies
    
    # ========================================================================
    # HELPER: IQR BOUNDS CALCULATION
    # ========================================================================
    
    def _calculate_iqr_bounds(self, series: pd.Series, multiplier: float = 2.0) -> Tuple[float, float, float, float, float]:
        """
        Calculate IQR bounds for outlier detection
        
        IQR Method:
        - Q1 = 25th percentile
        - Q3 = 75th percentile
        - IQR = Q3 - Q1
        - Lower bound = Q1 - 1.5 × IQR
        - Upper bound = Q3 + 1.5 × IQR
        
        Args:
            series: Data series
            multiplier: IQR multiplier (1.5 standard, 1.0 stricter, 2.5 balanced)
        
        Returns:
            (lower_bound, upper_bound, Q1, Q3, IQR)
        """
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        return lower_bound, upper_bound, Q1, Q3, IQR
    
    # ========================================================================
    # DETECTION 1: PRICE OUTLIERS
    # ========================================================================
    
    def detect_price_outliers(self):
        """
        Detect unit prices outside normal range for each product
        
        Business Context:
        - Product prices should be stable with small variations
        - Large price jump = possible invoice error or premium variant
        - Could indicate fraud (inflated price to hide tax)
        
        Cost if missed: ₹200K-₹500K billing disputes
        """
        print("  1. Detecting price outliers...")
        
        for product_id in self.df['product_id'].unique():
            product_shipments = self.df[self.df['product_id'] == product_id]
            
            if len(product_shipments) < 5:
                continue  # Need at least 5 data points for reliable statistics
            
            prices = product_shipments['unit_price']
            lower, upper, Q1, Q3, IQR = self._calculate_iqr_bounds(prices)
            
            outliers = product_shipments[
                (product_shipments['unit_price'] < lower) |
                (product_shipments['unit_price'] > upper)
            ]
            
            # Get product info
            product_info = self.product_catalog[
                self.product_catalog['id'] == product_id
            ]
            
            if len(product_info) == 0:
                continue
            
            product_info = product_info.iloc[0]
            expected_price = product_info['standard_price']
            
            for _, row in outliers.iterrows():
                deviation_pct = ((row['unit_price'] - expected_price) / expected_price * 100) if expected_price > 0 else 0
                invoice_discrepancy = row['quantity'] * (row['unit_price'] - expected_price)
                
                severity = 'HIGH' if abs(deviation_pct) > 50 else 'MEDIUM'
                
                self.anomalies.append({
                    'shipment_id': int(row['id']),
                    'anomaly_type': 'PRICE_OUTLIER',
                    'layer': 2,
                    'severity': severity,
                    'evidence': {
                        'product_name': product_info['name'],
                        'product_id': int(product_id),
                        'unit_price': float(row['unit_price']),
                        'standard_price': float(expected_price),
                        'deviation_percent': f"{deviation_pct:.2f}%",
                        'statistical_bounds': {
                            'lower_bound': float(lower),
                            'upper_bound': float(upper),
                            'Q1': float(Q1),
                            'Q3': float(Q3),
                            'IQR': float(IQR)
                        },
                        'sample_size': len(product_shipments)
                    },
                    'impact': f"Invoice discrepancy of ${invoice_discrepancy:.2f}. Possible error or premium variant.",
                    'recommendation': 'Review pricing with sales team. Verify against product catalog.'
                })
    
    # ========================================================================
    # DETECTION 2: TRANSIT TIME OUTLIERS
    # ========================================================================
    
    def detect_transit_time_outliers(self):
        """
        Detect unusually long transit times for routes
        
        Business Context:
        - Transit delays cost money and damage customer relationships
        - Delays >20 days = possible port issues, weather, mechanical failure
        - Wrong route recorded = suggests data entry error
        
        Cost if missed: $5K-$50K delay costs + buyer cancellations
        """
        print("  2. Detecting transit time outliers...")
        
        # Group by route
        route_groups = self.df.groupby(['origin_country', 'destination_country'])
        
        for (origin, destination), group in route_groups:
            if len(group) < 5:
                continue  # Need at least 5 data points for reliable statistics
            
            transit_times = group['days_in_transit']
            lower, upper, Q1, Q3, IQR = self._calculate_iqr_bounds(transit_times)
            
            outliers = group[
                (group['days_in_transit'] < lower) |
                (group['days_in_transit'] > upper)
            ]
            
            # Get expected transit time from routes table
            route_info = self.routes_df[
                (self.routes_df['origin'] == origin) &
                (self.routes_df['destination'] == destination)
            ]
            
            expected_days = route_info['avg_transit_days'].values[0] if len(route_info) > 0 else Q3
            
            for _, row in outliers.iterrows():
                delay_days = row['days_in_transit'] - expected_days
                severity = 'CRITICAL' if delay_days > 20 else 'MEDIUM'
                delay_cost = delay_days * 10  # Estimate $10/day delay cost
                
                self.anomalies.append({
                    'shipment_id': int(row['id']),
                    'anomaly_type': 'TRANSIT_TIME_OUTLIER',
                    'layer': 2,
                    'severity': severity,
                    'evidence': {
                        'route': f"{origin} → {destination}",
                        'actual_transit_days': int(row['days_in_transit']),
                        'expected_transit_days': int(expected_days),
                        'delay_days': int(delay_days),
                        'container_type': row.get('container_type', 'N/A'),
                        'statistical_bounds': {
                            'lower_bound': int(lower),
                            'upper_bound': int(upper),
                            'Q1': int(Q1),
                            'Q3': int(Q3),
                            'IQR': int(IQR)
                        },
                        'sample_size': len(group)
                    },
                    'impact': f"${delay_cost:.0f} delay cost. Buyer may cancel if delivery misses deadline.",
                    'recommendation': 'Check with shipping agent. Investigate root cause of delay.'
                })
    
    # ========================================================================
    # DETECTION 3: FREIGHT COST OUTLIERS
    # ========================================================================
    
    def detect_freight_outliers(self):
        """
        Detect unusually high freight costs for route+container combinations
        
        Business Context:
        - Freight costs vary by market conditions (fuel, demand, weather)
        - Consistent overspending = broker inefficiency or overcharging
        - CIF/DDP: seller bears cost, impacts profitability
        
        Cost if missed: ₹500K-₹2M wasted annually on freight
        """
        print("  3. Detecting freight cost outliers...")
        
        # Group by route + container type
        groups = self.df.groupby(['origin_country', 'destination_country', 'container_type'])
        
        for (origin, dest, container), group in groups:
            if len(group) < 5:
                continue  # Need at least 5 data points for reliable statistics
            
            freights = group['freight_cost']
            lower, upper, Q1, Q3, IQR = self._calculate_iqr_bounds(freights)
            
            # Skip detection if Q3 (75th percentile) is zero - insufficient data quality
            if Q3 == 0.0:
                continue
            
            outliers = group[
                (group['freight_cost'] < lower) |
                (group['freight_cost'] > upper)
            ]
            
            for _, row in outliers.iterrows():
                # Additional validation: flag only if freight > median × 1.5 AND absolute cost > $500
                if row['freight_cost'] <= (Q3 * 1.5) or row['freight_cost'] <= 500:
                    continue
                    
                excess_cost = row['freight_cost'] - Q3
                
                severity = 'HIGH' if excess_cost > 500 else 'MEDIUM'
                annual_impact = excess_cost * 12  # Project monthly to annual
                
                self.anomalies.append({
                    'shipment_id': int(row['id']),
                    'anomaly_type': 'FREIGHT_COST_OUTLIER',
                    'layer': 2,
                    'severity': severity,
                    'evidence': {
                        'route': f"{origin} → {dest}",
                        'container_type': container,
                        'freight_cost': float(row['freight_cost']),
                        'median_cost': float(Q3),
                        'excess_over_median': float(excess_cost),
                        'statistical_bounds': {
                            'lower_bound': float(lower),
                            'upper_bound': float(upper),
                            'Q1': float(Q1),
                            'Q3': float(Q3),
                            'IQR': float(IQR)
                        },
                        'sample_size': len(group)
                    },
                    'impact': f"Overpaid ${excess_cost:.2f}. Annualized impact: ${annual_impact:.2f}",
                    'recommendation': 'Negotiate rates with logistics provider. Consider alternatives.'
                })
    
    # ========================================================================
    # DETECTION 4: PAYMENT BEHAVIOR CHANGE
    # ========================================================================
    
    def detect_payment_behavior_change(self):
        """
        Detect buyers whose payment is getting slower
        
        Business Context:
        - Payment delays indicate financial distress
        - Early warning system for credit defaults
        - Cash flow impact: ₹500K-₹5M tied up waiting for payments
        
        Methodology:
        - Compare recent (last 3) payments vs historical average
        - Flag if recent > historical + 5 days
        
        Cost if missed: ₹500K-₹5M working capital loss
        """
        print("  4. Detecting payment behavior changes...")
        
        for buyer_id in self.df['buyer_id'].unique():
            buyer_shipments = self.df[self.df['buyer_id'] == buyer_id].sort_values('shipment_date')
            
            if len(buyer_shipments) < 5:
                continue  # Need history
            
            payment_times = buyer_shipments['days_to_payment'].dropna()
            
            if len(payment_times) < 3:
                continue
            
            # Split into old vs recent
            old_payments = payment_times.iloc[:-3]  # All but last 3
            recent_payments = payment_times.iloc[-3:]  # Last 3
            
            old_avg = old_payments.mean()
            recent_avg = recent_payments.mean()
            deterioration = recent_avg - old_avg
            
            # Flag if recent > old + 5 days
            if deterioration > 5:
                buyer_info = self.buyers_df[self.buyers_df['id'] == buyer_id]
                buyer_name = buyer_info['name'].values[0] if len(buyer_info) > 0 else f"Buyer {buyer_id}"
                expected_days = buyer_info['avg_payment_days'].values[0] if len(buyer_info) > 0 else old_avg
                
                cash_flow_impact = deterioration * 10000  # Estimate ₹10K per day
                
                self.anomalies.append({
                    'buyer_id': int(buyer_id),
                    'shipment_ids': buyer_shipments.iloc[-3:]['id'].tolist(),
                    'anomaly_type': 'PAYMENT_BEHAVIOR_DETERIORATION',
                    'layer': 2,
                    'severity': 'MEDIUM',
                    'evidence': {
                        'buyer_name': buyer_name,
                        'buyer_id': int(buyer_id),
                        'historical_avg_payment_days': float(old_avg),
                        'recent_avg_payment_days': float(recent_avg),
                        'deterioration_days': float(deterioration),
                        'last_3_payments': [int(x) for x in recent_payments.tolist()],
                        'expected_by_contract': float(expected_days),
                        'historical_sample_size': len(old_payments),
                        'recent_sample_size': len(recent_payments)
                    },
                    'impact': f"Cash flow delay: ₹{cash_flow_impact:.0f}. Signs of financial distress.",
                    'recommendation': 'Review buyer creditworthiness. Consider reducing credit limit or requesting payment guarantees.'
                })
    
    # ========================================================================
    # DETECTION 5: VOLUME SPIKES
    # ========================================================================
    
    def detect_volume_spikes(self):
        """
        Detect sudden volume changes per buyer
        
        Business Context:
        - Could be legitimate growth (good sign)
        - Could be fraud attempt (buyer exploiting credit limit)
        - Could indicate order cancellation risk
        
        Methodology:
        - Calculate monthly volumes
        - Flag if latest month > 2× average of previous months
        
        Cost if missed: ₹500K-₹5M fraud loss or contract issues
        """
        print("  5. Detecting volume spikes...")
        
        # Create month-year column
        self.df['month'] = pd.to_datetime(self.df['shipment_date']).dt.to_period('M')
        
        for buyer_id in self.df['buyer_id'].unique():
            buyer_shipments = self.df[self.df['buyer_id'] == buyer_id]
            
            # Calculate volume per month
            monthly_volumes = buyer_shipments.groupby('month')['quantity'].sum()
            
            if len(monthly_volumes) < 3:
                continue
            
            # Compare latest month to average of previous months
            avg_previous = monthly_volumes.iloc[:-1].mean()
            latest_volume = monthly_volumes.iloc[-1]
            
            if latest_volume > (avg_previous * 2):
                buyer_info = self.buyers_df[self.buyers_df['id'] == buyer_id]
                buyer_name = buyer_info['name'].values[0] if len(buyer_info) > 0 else f"Buyer {buyer_id}"
                credit_limit = buyer_info['credit_limit'].values[0] if len(buyer_info) > 0 else None
                
                spike_ratio = latest_volume / avg_previous
                
                self.anomalies.append({
                    'buyer_id': int(buyer_id),
                    'shipment_ids': buyer_shipments[buyer_shipments['month'] == monthly_volumes.index[-1]]['id'].tolist(),
                    'anomaly_type': 'VOLUME_SPIKE',
                    'layer': 2,
                    'severity': 'MEDIUM',
                    'evidence': {
                        'buyer_name': buyer_name,
                        'buyer_id': int(buyer_id),
                        'historical_avg_monthly_volume': float(avg_previous),
                        'latest_month_volume': float(latest_volume),
                        'spike_ratio': float(spike_ratio),
                        'buyer_credit_limit': float(credit_limit) if credit_limit else 'Unknown',
                        'months_analyzed': len(monthly_volumes)
                    },
                    'impact': 'Unexpected volume change. Could indicate opportunity or risk depending on buyer profile.',
                    'recommendation': 'Verify order authenticity. Check production capacity. Assess credit limit utilization.'
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
    
    try:
        shipments = pd.read_csv('../data/shipments.csv', dtype={'hs_code': str})
        buyers = pd.read_csv('../data/buyers.csv')
        products = pd.read_csv('../data/product_catalog.csv', dtype={'hs_code': str})
        routes = pd.read_csv('../data/routes.csv')
        
        # Run Layer 2
        detector = StatisticalDetector(shipments, products, routes, buyers)
        anomalies = detector.run_all_detections()
        
        print(f"\n{'='*70}")
        print(f"LAYER 2 RESULTS")
        print(f"{'='*70}")
        print(f"Total anomalies detected: {len(anomalies)}")
        
        # Print summary
        summary = detector.get_summary()
        print(f"\nBy Severity:")
        for severity, count in summary['by_severity'].items():
            print(f"  {severity}: {count}")
        
        print(f"\nBy Type:")
        for anom_type, count in summary['by_type'].items():
            print(f"  {anom_type}: {count}")
        
        # Print first 3 anomalies
        print(f"\nFirst 3 Anomalies:")
        for i, anom in enumerate(anomalies[:3], 1):
            print(f"\n{i}. {anom['anomaly_type']}")
            print(f"   Severity: {anom['severity']}")
            print(f"   Impact: {anom['impact'][:60]}...")
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nPlease run data_generator.py first to create test data.")
        sys.exit(1)