// pages/index.js
import { useEffect, useState } from 'react';
import socket from '../utils/socketClient';
import Link from 'next/link';

export default function Home() {
  const [tx, setTx] = useState([]);

    useEffect(() => {
      //handle postgress db first
      const fetchPostgress = async () => {
        const res = await fetch('/api/postgressPrediction');
        if (res.ok) {
          const data = await res.json();
          setTx((prev)=>{
            // Ensure 'data' is an array, and merge it with previous transactions
            const newTx = Array.isArray(data) ? data : [data];
            return [...prev, ...newTx];
          });
        }
      };
      fetchPostgress();
      // handle using websocket

      fetch('/api/socket');
      const handleUpdate = (data) => {
        setTx((prev) => {
          // Ensure 'data' is an array, and merge it with previous transactions
          console.log("new data available ")
          const newTx = Array.isArray(data) ? data : [data];
          return [...newTx, ...prev];
        });
      };

      socket.on('predictionUpdate', handleUpdate);
      
      return () => {
        socket.off('predictionUpdate', handleUpdate);
      };
    }, []);

  return (
    <div style={styles.container}>
      <h2 style={styles.heading}>🚨 High-Value Transactions</h2>
      <div style={{ textAlign: 'center', marginBottom: '20px' }}>
        <Link href="/performance">
          <a style={{ fontSize: '1rem', color: '#1890ff', textDecoration: 'underline' }}>
            Performance Metrics
          </a>
        </Link>
      </div>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Timestamp</th>
            {[...Array(8)].map((_, i) => (
              <th key={i} style={styles.th}>Rule {i + 1}</th>
            ))}
            <th style={styles.th}>Prediction</th>
            <th style={styles.th}>Fraud Score</th>
          </tr>
        </thead>
        <tbody>
          {tx.map((t, i) => (
            <tr key={i} style={i % 2 === 0 ? styles.rowEven : styles.rowOdd}>
              <td style={styles.td}>{t.txn_id ?? 'N/A'}</td>
              {[...Array(8)].map((_, idx) => (
                <td key={idx} style={styles.td}>{t[`score_rule${idx + 1}`] ?? '-'}</td>
              ))}
              <td style={styles.td}>
                <span style={{
                  ...styles.badge,
                  backgroundColor: t.fraud_prediction ? '#ff4d4f' : '#52c41a'
                }}>
                  {t.fraud_prediction ? 'FRAUD' : 'SAFE'}
                </span>
              </td>
              <td style={{ ...styles.td, fontWeight: 'bold' }}>
                {t.fraud_score?.toFixed(2) ?? '-'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// 🔵 CSS-in-JS styles
const styles = {
  container: {
    padding: '40px',
    fontFamily: 'Arial, sans-serif',
    backgroundColor: '#f9f9f9',
    minHeight: '100vh',
  },
  heading: {
    marginBottom: '20px',
    color: '#333',
    textAlign: 'center',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    boxShadow: '0 0 10px rgba(0,0,0,0.1)',
    backgroundColor: '#fff',
    borderRadius: '6px',
    overflow: 'hidden',
  },
  th: {
    backgroundColor: '#001529',
    color: '#fff',
    padding: '12px',
    textAlign: 'center',
    fontWeight: '600',
  },
  td: {
    padding: '10px',
    textAlign: 'center',
    borderBottom: '1px solid #eee',
  },
  rowEven: {
    backgroundColor: '#ffffff',
  },
  rowOdd: {
    backgroundColor: '#f0f2f5',
  },
  badge: {
    padding: '4px 10px',
    color: '#fff',
    borderRadius: '12px',
    fontSize: '0.8rem',
    fontWeight: 'bold',
    display: 'inline-block',
  }
};
