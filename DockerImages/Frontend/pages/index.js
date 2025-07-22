import { useEffect, useState } from 'react';

export default function Home() {
  const [tx, setTx] = useState([]);

  useEffect(() => {
    const interval = setInterval(async () => {
      const res = await fetch('/api/transactions');
      const data = await res.json();
      setTx(data);
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      <h2>High-Value Transactions</h2>
      <ul>
        {tx.map((t, i) => (
          <li key={i}>
            {t.timestamp} — ${t.score_rule1}, ${t.score_rule2}, ${t.score_rule3}, ${t.score_rule4}, ${t.score_rule5}, ${t.score_rule6}, ${t.score_rule7}, ${t.score_rule8}
          </li>
        ))}
      </ul>
    </div>
  );
}
