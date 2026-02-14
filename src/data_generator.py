import pandas as pd
import json
import random
from datetime import datetime, timedelta
import numpy as np
import os

random.seed(42)
np.random.seed(42)


def generate_buyers():
    """Create buyer data"""
    print("Generating buyers data...")
    
    buyers = [
        {'id': 1, 'name': 'Acme Electronics Ltd', 'country': 'UAE', 'business_type': 'Distributor', 'credit_limit': 500000, 'avg_payment_days': 15, 'payment_reliability': 'Excellent', 'contact_email': 'procurement@acme-elec.ae'},
        {'id': 2, 'name': 'Global Imports Corp', 'country': 'USA', 'business_type': 'Importer', 'credit_limit': 750000, 'avg_payment_days': 30, 'payment_reliability': 'Good', 'contact_email': 'orders@globalimports.us'},
        {'id': 3, 'name': 'TechTrade Singapore', 'country': 'Singapore', 'business_type': 'Distributor', 'credit_limit': 600000, 'avg_payment_days': 20, 'payment_reliability': 'Good', 'contact_email': 'procurement@techtrade.sg'},
        {'id': 4, 'name': 'European Commerce GmbH', 'country': 'Germany', 'business_type': 'Wholesaler', 'credit_limit': 900000, 'avg_payment_days': 45, 'payment_reliability': 'Average', 'contact_email': 'orders@eurocomm.de'},
        {'id': 5, 'name': 'Premium Retail UK Ltd', 'country': 'United Kingdom', 'business_type': 'Retailer', 'credit_limit': 400000, 'avg_payment_days': 25, 'payment_reliability': 'Good', 'contact_email': 'supply@premiumretail.uk'},
        {'id': 6, 'name': 'Asia Pacific Trading', 'country': 'Australia', 'business_type': 'Distributor', 'credit_limit': 550000, 'avg_payment_days': 35, 'payment_reliability': 'Average', 'contact_email': 'procurement@aptrade.com.au'}
    ]
    
    return pd.DataFrame(buyers)


def generate_products():
    """Create product catalog"""
    print("Generating product catalog...")
    
    products = [
        {'id': 1, 'name': 'Cotton T-Shirts (Plain)', 'category': 'Textiles', 'description': 'Plain cotton t-shirts, 100% cotton, various colors', 'material': 'Cotton', 'standard_price': 5.50, 'hs_code': '62091000', 'hs_code_description': 'T-shirts and singlets of cotton, knitted'},
        {'id': 2, 'name': 'Polyester Dress Shirts', 'category': 'Textiles', 'description': 'Formal dress shirts, 65% polyester 35% cotton', 'material': 'Polyester/Cotton', 'standard_price': 12.50, 'hs_code': '62035200', 'hs_code_description': 'Shirts of man-made fibres, not knitted'},
        {'id': 3, 'name': 'Electronic Control Units', 'category': 'Electronics', 'description': 'Industrial ECU for automotive applications', 'material': 'Electronic Components', 'standard_price': 85.00, 'hs_code': '84713000', 'hs_code_description': 'Semiconductor devices, electronic assemblies'},
        {'id': 4, 'name': 'Plastic Storage Containers', 'category': 'Plastics', 'description': 'Food-grade plastic containers, stackable', 'material': 'Polypropylene', 'standard_price': 2.25, 'hs_code': '39269090', 'hs_code_description': 'Articles of plastics, n.e.c.'},
        {'id': 5, 'name': 'Wooden Chair Frames', 'category': 'Wood Furniture', 'description': 'Solid teak wood chair frames, ready for upholstery', 'material': 'Teak Wood', 'standard_price': 45.00, 'hs_code': '94015090', 'hs_code_description': 'Seats with wooden frames'},
        {'id': 6, 'name': 'Stainless Steel Fasteners', 'category': 'Metal Products', 'description': 'Various bolts, nuts, screws (M5-M12)', 'material': 'Stainless Steel', 'standard_price': 0.75, 'hs_code': '73181500', 'hs_code_description': 'Bolts, nuts, screws of iron or steel'},
        {'id': 7, 'name': 'Ceramic Tiles (Glazed)', 'category': 'Ceramics', 'description': '300x300mm glazed ceramic tiles for flooring', 'material': 'Ceramic', 'standard_price': 3.50, 'hs_code': '69099000', 'hs_code_description': 'Articles of ceramic materials, n.e.c.'},
        {'id': 8, 'name': 'Rubber Gaskets', 'category': 'Rubber Products', 'description': 'Industrial rubber gaskets, various sizes', 'material': 'Natural Rubber', 'standard_price': 1.20, 'hs_code': '40169000', 'hs_code_description': 'Articles of rubber, n.e.c.'},
        {'id': 9, 'name': 'Acrylic Winter Hoodies', 'category': 'Textiles', 'description': 'Knitted acrylic hoodies, crew neck, unisex', 'material': 'Acrylic', 'standard_price': 18.75, 'hs_code': '60022000', 'hs_code_description': 'Fabrics of artificial fibres, knitted'},
        {'id': 10, 'name': 'LED Light Bulbs', 'category': 'Electronics', 'description': 'E27 LED bulbs, 12W, 1000 lumens, warm white', 'material': 'Electronic/Plastic', 'standard_price': 4.50, 'hs_code': '85395000', 'hs_code_description': 'Light-emitting diodes and lamps'},
        {'id': 11, 'name': 'Brass Door Handles', 'category': 'Hardware', 'description': 'Decorative brass door handles, various designs', 'material': 'Brass', 'standard_price': 8.25, 'hs_code': '83062000', 'hs_code_description': 'Handles and knobs of brass'},
        {'id': 12, 'name': 'Spice Seasoning Mix', 'category': 'Food Products', 'description': 'Blended Indian spice mix, 500g pack', 'material': 'Organic Spices', 'standard_price': 6.75, 'hs_code': '09101900', 'hs_code_description': 'Spices, dried (excluding pepper)'}
    ]
    
    df = pd.DataFrame(products)
    hs_code_list = [str(p['hs_code']) for p in products]
    df['hs_code'] = pd.Series(hs_code_list, dtype='object')
    
    return df


def generate_routes():
    """Create trade routes"""
    print("Generating routes data...")
    
    routes = [
        {'origin': 'India', 'destination': 'UAE', 'origin_port': 'Mundra Port', 'destination_port': 'Jebel Ali Port', 'avg_transit_days': 10, 'distance_km': 1850, 'shipping_lanes': 'Arabian Sea'},
        {'origin': 'India', 'destination': 'Singapore', 'origin_port': 'Port of Cochin', 'destination_port': 'Port of Singapore', 'avg_transit_days': 7, 'distance_km': 2200, 'shipping_lanes': 'Arabian Sea / Indian Ocean'},
        {'origin': 'India', 'destination': 'USA', 'origin_port': 'JNPT Mumbai', 'destination_port': 'Port of Long Beach', 'avg_transit_days': 30, 'distance_km': 11200, 'shipping_lanes': 'Trans-Pacific'},
        {'origin': 'India', 'destination': 'UK', 'origin_port': 'JNPT Mumbai', 'destination_port': 'Port of Rotterdam', 'avg_transit_days': 25, 'distance_km': 9100, 'shipping_lanes': 'Suez / Mediterranean'},
        {'origin': 'India', 'destination': 'Germany', 'origin_port': 'Port of Cochin', 'destination_port': 'Port of Hamburg', 'avg_transit_days': 27, 'distance_km': 9400, 'shipping_lanes': 'Suez / Mediterranean'},
        {'origin': 'India', 'destination': 'Australia', 'origin_port': 'Port of Chennai', 'destination_port': 'Port of Melbourne', 'avg_transit_days': 14, 'distance_km': 5500, 'shipping_lanes': 'Indian Ocean / Tasman Sea'},
        {'origin': 'India', 'destination': 'China', 'origin_port': 'JNPT Mumbai', 'destination_port': 'Port of Shanghai', 'avg_transit_days': 12, 'distance_km': 2800, 'shipping_lanes': 'Bay of Bengal / South China Sea'},
        {'origin': 'India', 'destination': 'Hong Kong', 'origin_port': 'Port of Cochin', 'destination_port': 'Port of Hong Kong', 'avg_transit_days': 10, 'distance_km': 2500, 'shipping_lanes': 'Indian Ocean / South China Sea'},
        {'origin': 'India', 'destination': 'Malaysia', 'origin_port': 'JNPT Mumbai', 'destination_port': 'Port of Port Klang', 'avg_transit_days': 8, 'distance_km': 1900, 'shipping_lanes': 'Arabian Sea / Malacca Strait'},
        {'origin': 'India', 'destination': 'Japan', 'origin_port': 'Port of Chennai', 'destination_port': 'Port of Tokyo', 'avg_transit_days': 15, 'distance_km': 3600, 'shipping_lanes': 'Indian Ocean / Pacific Ocean'},
        {'origin': 'India', 'destination': 'Canada', 'origin_port': 'JNPT Mumbai', 'destination_port': 'Port of Vancouver', 'avg_transit_days': 32, 'distance_km': 12500, 'shipping_lanes': 'Trans-Pacific'},
        {'origin': 'India', 'destination': 'Netherlands', 'origin_port': 'Port of Cochin', 'destination_port': 'Port of Rotterdam', 'avg_transit_days': 26, 'distance_km': 9200, 'shipping_lanes': 'Suez / Mediterranean / Atlantic'}
    ]
    
    return pd.DataFrame(routes)


def generate_base_shipments(buyers_df, products_df, routes_df):
    """Generate 250 shipments - ORIGINAL CODE UNCHANGED"""
    print("Generating 250 base shipments...")
    
    shipments = []
    start_date = datetime.now() - timedelta(days=90)
    
    incoterms = ['FOB', 'CIF', 'EXW', 'DDP']
    payment_statuses = ['received', 'pending', 'overdue']
    customs_statuses = ['cleared', 'pending', 'rejected']
    container_types = ['20ft', '40ft', 'LCL']
    
    for i in range(250):
        buyer = buyers_df.sample(1).iloc[0]
        product = products_df.sample(1).iloc[0]
        route = routes_df.sample(1).iloc[0]
        incoterm = random.choice(incoterms)
        payment_status = random.choice(payment_statuses)
        customs_status = random.choice(customs_statuses)
        container_type = random.choice(container_types)
        
        quantity = random.randint(100, 5000)
        unit_price = product['standard_price'] * round(random.uniform(0.8, 1.2), 2)
        total_fob = round(quantity * unit_price, 2)
        
        if incoterm in ['FOB', 'EXW']:
            freight_cost = 0
            insurance_amount = 0
        else:
            if container_type == '20ft':
                freight_base = random.uniform(1500, 3500)
            elif container_type == '40ft':
                freight_base = random.uniform(2000, 4500)
            else:
                freight_base = random.uniform(500, 2000)
            
            freight_cost = freight_base
            
            if incoterm == 'DDP':
                insurance_amount = total_fob * random.uniform(0.005, 0.015)
            elif incoterm == 'CIF':
                insurance_amount = total_fob * random.uniform(0.005, 0.015) if random.random() > 0.5 else 0
            else:
                insurance_amount = 0
        
        days_in_transit = route['avg_transit_days'] + random.randint(-2, 5)
        days_in_transit = max(1, days_in_transit)
        
        shipment_date = start_date + timedelta(days=random.randint(0, 90))
        
        if customs_status == 'cleared':
            drawback_amount = total_fob * random.uniform(0.03, 0.05)
        else:
            drawback_amount = 0
        
        if payment_status == 'received':
            days_to_payment = buyer['avg_payment_days'] + random.randint(-5, 10)
            days_to_payment = max(1, days_to_payment)
        elif payment_status == 'pending':
            days_to_payment = random.randint(-30, 15)
        else:
            days_to_payment = buyer['avg_payment_days'] + random.randint(10, 60)
        
        shipments.append({
            'id': i + 1,
            'buyer_id': int(buyer['id']),
            'product_id': int(product['id']),
            'quantity': quantity,
            'unit_price': round(unit_price, 2),
            'total_fob': round(total_fob, 2),
            'incoterm': incoterm,
            'freight_cost': round(freight_cost, 2),
            'insurance_amount': round(insurance_amount, 2),
            'hs_code': str(product['hs_code']),
            'origin_country': route['origin'],
            'destination_country': route['destination'],
            'origin_port': route['origin_port'],
            'destination_port': route['destination_port'],
            'shipment_date': shipment_date.strftime('%Y-%m-%d'),
            'days_in_transit': days_in_transit,
            'customs_status': customs_status,
            'drawback_amount': round(drawback_amount, 2),
            'payment_status': payment_status,
            'days_to_payment': days_to_payment if payment_status != 'pending' else None,
            'container_type': container_type
        })
    
    return pd.DataFrame(shipments)


def plant_anomalies(shipments_df, products_df):
    """
    FIXED: Plant anomalies with correct product_id changes for HS code mismatches
    
    Layer 1: 7 anomalies - UNCHANGED
    Layer 2: 3 anomalies - UNCHANGED
    Layer 3: 2 anomalies - FIXED (change product_id so LLM can detect mismatch)
    """
    
    planted_anomalies = []
    
    # ========== LAYER 1: RULE-BASED ANOMALIES (7) - UNCHANGED ==========
    
    # 1. PRICE_MISMATCH
    idx = 11
    original_total = shipments_df.loc[idx, 'total_fob']
    wrong_total = original_total + 1500
    shipments_df.loc[idx, 'total_fob'] = round(wrong_total, 2)
    
    planted_anomalies.append({
        'shipment_id': 12,
        'anomaly_type': 'PRICE_MISMATCH',
        'layer': 1,
        'severity': 'HIGH',
        'description': 'total_fob does not match qty × unit_price',
        'expected_value': round(original_total, 2),
        'actual_value': round(wrong_total, 2),
        'discrepancy': 1500,
        'cost_if_missed': 1500
    })
    
    # 2. INCOTERM_FREIGHT_MISMATCH
    idx = 32
    shipments_df.loc[idx, 'incoterm'] = 'CIF'
    shipments_df.loc[idx, 'freight_cost'] = 0
    planted_anomalies.append({'shipment_id': 33, 'anomaly_type': 'INCOTERM_FREIGHT_MISMATCH', 'layer': 1, 'severity': 'CRITICAL', 'description': 'CIF incoterm but freight_cost = 0', 'incoterm': 'CIF', 'freight_cost': 0, 'cost_if_missed': 500000})
    
    # 3. INCOTERM_EXW_ERROR
    idx = 54
    shipments_df.loc[idx, 'incoterm'] = 'EXW'
    shipments_df.loc[idx, 'freight_cost'] = 2500
    planted_anomalies.append({'shipment_id': 55, 'anomaly_type': 'INCOTERM_EXW_ERROR', 'layer': 1, 'severity': 'HIGH', 'description': 'EXW incoterm but seller paid freight', 'incoterm': 'EXW', 'freight_cost': 2500, 'cost_if_missed': 2500})
    
    # 4. INVALID_DRAWBACK_CLAIM
    idx = 90
    shipments_df.loc[idx, 'customs_status'] = 'rejected'
    shipments_df.loc[idx, 'drawback_amount'] = 75000
    planted_anomalies.append({'shipment_id': 91, 'anomaly_type': 'INVALID_DRAWBACK_CLAIM', 'layer': 1, 'severity': 'CRITICAL', 'description': 'Shipment rejected but drawback claimed', 'customs_status': 'rejected', 'drawback_claimed': 75000, 'cost_if_missed': 112500})
    
    # 5. MISSING_PAYMENT_DATE
    idx = 43
    shipments_df.loc[idx, 'payment_status'] = 'received'
    shipments_df.loc[idx, 'days_to_payment'] = np.nan
    planted_anomalies.append({'shipment_id': 44, 'anomaly_type': 'MISSING_PAYMENT_DATE', 'layer': 1, 'severity': 'MEDIUM', 'description': 'Payment marked received but no date', 'payment_status': 'received', 'days_to_payment': None, 'cost_if_missed': 250000})
    
    # 6. INVALID_HS_CODE_FORMAT
    idx = 14
    shipments_df.loc[idx, 'hs_code'] = '8471300'
    planted_anomalies.append({'shipment_id': 15, 'anomaly_type': 'INVALID_HS_CODE_FORMAT', 'layer': 1, 'severity': 'CRITICAL', 'description': 'HS code has 7 digits instead of 8', 'hs_code': '8471300', 'expected_length': 8, 'actual_length': 7, 'cost_if_missed': 500000})
    
    # 7. EXCESSIVE_INSURANCE
    idx = 66
    excessive_insurance = shipments_df.loc[idx, 'total_fob'] * 0.05
    shipments_df.loc[idx, 'insurance_amount'] = round(excessive_insurance, 2)
    planted_anomalies.append({'shipment_id': 67, 'anomaly_type': 'EXCESSIVE_INSURANCE', 'layer': 1, 'severity': 'LOW', 'description': 'Insurance is 5% of FOB (normal 0.5-1.5%)', 'insurance_amount': round(excessive_insurance, 2), 'fob_value': round(shipments_df.loc[idx, 'total_fob'], 2), 'insurance_pct': 5.0, 'cost_if_missed': 2500})
    
    # ========== LAYER 2: STATISTICAL ANOMALIES (3) - UNCHANGED ==========
    
    # 8. PRICE_OUTLIER
    idx = 119
    product_id = shipments_df.loc[idx, 'product_id']
    normal_price = shipments_df[shipments_df['product_id'] == product_id]['unit_price'].mean()
    extreme_price = normal_price * 3
    shipments_df.loc[idx, 'unit_price'] = round(extreme_price, 2)
    shipments_df.loc[idx, 'total_fob'] = round(shipments_df.loc[idx, 'quantity'] * extreme_price, 2)
    planted_anomalies.append({'shipment_id': 120, 'anomaly_type': 'PRICE_OUTLIER', 'layer': 2, 'severity': 'HIGH', 'description': 'Unit price is 3x higher than normal', 'normal_price': round(normal_price, 2), 'anomalous_price': round(extreme_price, 2), 'deviation_pct': 200, 'cost_if_missed': 50000})
    
    # 9. TRANSIT_TIME_OUTLIER
    idx = 179
    shipments_df.loc[idx, 'days_in_transit'] = 60
    planted_anomalies.append({'shipment_id': 180, 'anomaly_type': 'TRANSIT_TIME_OUTLIER', 'layer': 2, 'severity': 'MEDIUM', 'description': 'Transit time is 60 days (extreme)', 'days_in_transit': 60, 'expected_range': '25-35 days', 'delay': 30, 'cost_if_missed': 100000})
    
    # 10. FREIGHT_COST_OUTLIER
    # idx = 199
    # shipments_df.loc[idx, 'freight_cost'] = round(shipments_df.loc[idx, 'freight_cost'] * 4.0, 2)
    # planted_anomalies.append({'shipment_id': 200, 'anomaly_type': 'FREIGHT_COST_OUTLIER', 'layer': 2, 'severity': 'MEDIUM', 'description': 'Freight cost is 2.5x higher', 'freight_cost': round(shipments_df.loc[idx, 'freight_cost'], 2), 'cost_if_missed': 5000})
    idx_200 = 199
    # 1. Force Price to be NORMAL (prevents accidental PRICE_OUTLIER FP)
    prod_id_200 = shipments_df.at[idx_200, 'product_id']
    std_price_200 = products_df.loc[products_df['id'] == prod_id_200, 'standard_price'].values[0]
    shipments_df.at[idx_200, 'unit_price'] = std_price_200
    shipments_df.at[idx_200, 'total_fob'] = shipments_df.at[idx_200, 'quantity'] * std_price_200
    
    # 2. Make Freight EXTREME (beats the 3.5x IQR threshold)
    shipments_df.at[idx_200, 'freight_cost'] = round(shipments_df.at[idx_200, 'freight_cost'] * 6.0, 2)
    
    # 3. Label it correctly to match the engine
    planted_anomalies.append({
        'shipment_id': 200, 'anomaly_type': 'FREIGHT_COST_OUTLIER', 
        'layer': 2, 'severity': 'MEDIUM'
    })

    # ========== LAYER 3: LLM ANOMALIES (2) - FIXED ==========
    # FIX: Change product_id so LLM sees clear mismatch between product material and HS code
    
    # 11. HS_CODE_PRODUCT_MISMATCH #1 (Shipment #120)
    # Change product to Electronics (id=3), change HS to textile (62)
    idx = 119
    shipments_df.loc[idx, 'product_id'] = 3  # Electronic Control Units
    shipments_df.loc[idx, 'hs_code'] = '62091000'  # Textile HS code
    
    product_3 = products_df[products_df['id'] == 3].iloc[0]
    
    planted_anomalies.append({
        'shipment_id': 120,
        'anomaly_type': 'HS_CODE_PRODUCT_MISMATCH',
        'layer': 3,
        'severity': 'HIGH',
        'description': f'HS code 62091000 (textiles) but product is {product_3["name"]} (material: {product_3["material"]})',
        'claimed_hs_code': '62091000',
        'claimed_hs_description': 'Cotton T-shirts (textiles)',
        'actual_product': product_3['name'],
        'actual_material': product_3['material'],
        'actual_product_id': 3,
        'correct_hs_code': '84713000',
        'cost_if_missed': 800000
    })
    
    
    # 12. HS_CODE_PRODUCT_MISMATCH #2 (Shipment #200)
    # Change product to Metal Fasteners (id=6), change HS to textile (60)
    idx = 199
    shipments_df.loc[idx, 'product_id'] = 6  # Stainless Steel Fasteners
    shipments_df.loc[idx, 'hs_code'] = '60022000'  # Different textile HS code
    
    product_6 = products_df[products_df['id'] == 6].iloc[0]
    
    planted_anomalies.append({
        'shipment_id': 200,
        'anomaly_type': 'HS_CODE_PRODUCT_MISMATCH',
        'layer': 3,
        'severity': 'HIGH',
        'description': f'HS code 60022000 (textiles) but product is {product_6["name"]} (material: {product_6["material"]})',
        'claimed_hs_code': '60022000',
        'claimed_hs_description': 'Textile fabrics',
        'actual_product': product_6['name'],
        'actual_material': product_6['material'],
        'actual_product_id': 6,
        'correct_hs_code': '73181500',
        'cost_if_missed': 800000
    })
    
    return shipments_df, planted_anomalies


def save_data(buyers_df, products_df, routes_df, shipments_df, planted_anomalies, output_dir='../data'):
    """Save all data"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    print("\nSaving data to CSV files...")
    
    buyers_df.to_csv(f'{output_dir}/buyers.csv', index=False)
    print(f"[OK] Saved {len(buyers_df)} buyers")
    
    products_df['hs_code'] = products_df['hs_code'].astype(str)
    products_df.to_csv(f'{output_dir}/product_catalog.csv', index=False, quoting=1)
    print(f"[OK] Saved {len(products_df)} products")
    
    routes_df.to_csv(f'{output_dir}/routes.csv', index=False)
    print(f"[OK] Saved {len(routes_df)} routes")
    
    shipments_df['hs_code'] = shipments_df['hs_code'].astype(str)
    shipments_df.to_csv(f'{output_dir}/shipments.csv', index=False, quoting=1)
    print(f"[OK] Saved {len(shipments_df)} shipments")
    
    with open(f'{output_dir}/planted_anomalies.json', 'w') as f:
        json.dump(planted_anomalies, f, indent=2)
    print(f"[OK] Saved {len(planted_anomalies)} planted anomalies")
    
    print("\n" + "="*70)
    print("DATA GENERATION COMPLETE!")
    print(f"Anomalies: Layer1=7, Layer2=3, Layer3=2 (total=12)")
    print("="*70 + "\n")


def main():
    """Main execution"""
    
    print("="*70)
    print("SHIPMENT ANOMALY DETECTION - DATA GENERATION (FIXED FOR LLM)")
    print("="*70 + "\n")
    
    buyers_df = generate_buyers()
    products_df = generate_products()
    routes_df = generate_routes()
    shipments_df = generate_base_shipments(buyers_df, products_df, routes_df)
    shipments_df, planted_anomalies = plant_anomalies(shipments_df, products_df)
    
    save_data(buyers_df, products_df, routes_df, shipments_df, planted_anomalies)


if __name__ == "__main__":
    main()