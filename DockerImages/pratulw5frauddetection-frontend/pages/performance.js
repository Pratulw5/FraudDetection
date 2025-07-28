// pages/performance.js
import { useEffect, useState } from 'react';
import socket from '../utils/socketClient';
import Link from 'next/link';

export default function Reports() {
    const [latestMetric, setLatestMetric] = useState(null);

    useEffect(() => {
        // Initiate socket connection
        const fetchPostgress = async () => {
        const res = await fetch('/api/postgressMetric');
        if (res.ok) {
          const data = await res.json();
          setLatestMetric(data);
        }   
        };
        fetchPostgress();

        fetch('/api/socket'); // Triggers the WS server setup (hot init)

        // Log connection status
        socket.on('connect', () => {
            console.log('Socket connected');
        });

        // Log the received data
        socket.on('metricsUpdate', (data) => {
            setLatestMetric(data);
        });
    
        return () => {
                socket.off('metricsUpdate', handleUpdate);
        };
    }, []);

    return (
        <div style={styles.container}>
            <h1 style={styles.heading}>📊 Reports Dashboard</h1>
            <p style={styles.paragraph}>Here you can add analytics, historical data, or charts related to transactions.</p>
            <div style={{ textAlign: 'center', marginBottom: '20px' }}>
                <Link href="/">
                    <a style={{ fontSize: '1rem', color: '#1890ff', textDecoration: 'underline' }}>
                        Home
                    </a>
                </Link>
            </div>
            
            {latestMetric ? (
                <table style={styles.table}>
                    <thead>
                        <tr>
                            <th style={styles.th}>Timestamp</th>
                            <th style={styles.th}>TP</th>
                            <th style={styles.th}>FP</th>
                            <th style={styles.th}>TN</th>
                            <th style={styles.th}>FN</th>
                            <th style={styles.th}>Precision</th>
                            <th style={styles.th}>Recall</th>
                            <th style={styles.th}>Accuracy</th>
                            <th style={styles.th}>F1 Score</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style={styles.td}>{latestMetric.timestamp}</td>
                            <td style={styles.td}>{latestMetric.tp}</td>
                            <td style={styles.td}>{latestMetric.fp}</td>
                            <td style={styles.td}>{latestMetric.tn}</td>
                            <td style={styles.td}>{latestMetric.fn}</td>
                            <td style={styles.td}>{latestMetric.precision}</td>
                            <td style={styles.td}>{latestMetric.recall}</td>
                            <td style={styles.td}>{latestMetric.accuracy}</td>
                            <td style={styles.td}>{latestMetric.f1_score}</td>
                        </tr>
                    </tbody>
                </table>
            ) : (
                <p>No metrics available yet.</p>
            )}

        </div>
    );
}

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
  paragraph: {
    fontSize: '1.1rem',
    color: '#555',
    textAlign: 'center',
  },
  table: {
    width: '80%',
    margin: '0 auto',
    borderCollapse: 'collapse',
  },
  th: {
    backgroundColor: '#1890ff',
    color: 'white',
    padding: '8px 12px',
    textAlign: 'center',
  },
  td: {
    padding: '8px 12px',
    textAlign: 'center',
    border: '1px solid #ddd',
  }
};
