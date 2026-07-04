export function Mark({ size = 44, plate = 'dark', style }) {
  // Coin & slot: solid coin poised over a slot cut clean through the plate.
  // plate 'dark' = ink plate w/ brass coin (for light surroundings);
  // plate 'light' = paper plate w/ dark-brass coin (for dark surroundings).
  const s = size / 48;
  const colors = plate === 'dark'
    ? { plate: '#1C1A16', coin: '#C79B3B' }
    : { plate: '#E9E4DB', coin: '#8A6A1C' };
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" style={style}>
      <defs>
        <mask id={'mb-slot-' + plate}>
          <rect width="48" height="48" fill="white" />
          <rect x="12" y="23.5" width="24" height="4.5" rx="2.25" fill="black" />
        </mask>
      </defs>
      <rect width="48" height="48" rx="11" fill={colors.plate} mask={'url(#mb-slot-' + plate + ')'} />
      <circle cx="24" cy="13" r="6.5" fill={colors.coin} />
    </svg>
  );
}
