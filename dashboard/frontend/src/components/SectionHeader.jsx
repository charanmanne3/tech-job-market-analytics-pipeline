export default function SectionHeader({ title, subtitle }) {
  return (
    <div className="mb-5">
      <h2 className="text-xl font-bold text-white">{title}</h2>
      {subtitle && (
        <p className="text-sm text-slate-400 mt-0.5">{subtitle}</p>
      )}
    </div>
  );
}
