import { useState } from 'react'
import { NavLink, Route, Routes } from 'react-router-dom'

import Overview from './views/Overview'
import RealtimemetricView from './views/realtimemetric'
import DashboardView from './views/dashboard'
import DrilldownView from './views/drilldown'
import ThresholdView from './views/threshold'
import AnomalydetectView from './views/anomalydetect'
import DataserviceView from './views/dataservice'
import AlarmhistoryView from './views/alarmhistory'
import ComparisonView from './views/comparison'
import AggregateView from './views/aggregate'
import FilterpresetView from './views/filterpreset'
import UserinsightView from './views/userinsight'
import EventanalysisView from './views/eventanalysis'
import SegmentprofileView from './views/segmentprofile'
import TrendforecastView from './views/trendforecast'
import ViztemplateView from './views/viztemplate'
import SubscriptionView from './views/subscription'
import DimensionmapView from './views/dimensionmap'
import QualityscoreView from './views/qualityscore'

const navItems = [{ label: "运营概览", path: "/" }, { label: "实时指标", path: "/realtimemetric" }, { label: "监控大盘", path: "/dashboard" }, { label: "多维下钻", path: "/drilldown" }, { label: "阈值管理", path: "/threshold" }, { label: "异常检测", path: "/anomalydetect" }, { label: "数据服务", path: "/dataservice" }, { label: "告警历史", path: "/alarmhistory" }, { label: "同比环比", path: "/comparison" }, { label: "聚合配置", path: "/aggregate" }, { label: "筛选预设", path: "/filterpreset" }, { label: "用户洞察", path: "/userinsight" }, { label: "事件分析", path: "/eventanalysis" }, { label: "分群画像", path: "/segmentprofile" }, { label: "趋势预测", path: "/trendforecast" }, { label: "可视化模板", path: "/viztemplate" }, { label: "订阅推送", path: "/subscription" }, { label: "维度映射", path: "/dimensionmap" }, { label: "数据质量分", path: "/qualityscore" }]

export default function App() {
  const [operator] = useState('值班管理员')

  return (
    <div className="app-shell">
      <aside className="app-side">
        <h1 className="app-title">实时指标计算与监控大盘平台</h1>
        <nav className="nav-list">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === '/'}
              className={({ isActive }) => `nav-item${isActive ? ' active' : ''}`}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="app-main">
        <header className="app-head">
          <span className="head-desc">面向实时指标计算引擎、多维度下钻分析、告警阈值管理、指标异常检测、数据服务开放与可视化看板配置的数据分析与应用工程平台。</span>
          <span className="head-user">当前值班：{operator}</span>
        </header>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/realtimemetric" element={<RealtimemetricView />} />
          <Route path="/dashboard" element={<DashboardView />} />
          <Route path="/drilldown" element={<DrilldownView />} />
          <Route path="/threshold" element={<ThresholdView />} />
          <Route path="/anomalydetect" element={<AnomalydetectView />} />
          <Route path="/dataservice" element={<DataserviceView />} />
          <Route path="/alarmhistory" element={<AlarmhistoryView />} />
          <Route path="/comparison" element={<ComparisonView />} />
          <Route path="/aggregate" element={<AggregateView />} />
          <Route path="/filterpreset" element={<FilterpresetView />} />
          <Route path="/userinsight" element={<UserinsightView />} />
          <Route path="/eventanalysis" element={<EventanalysisView />} />
          <Route path="/segmentprofile" element={<SegmentprofileView />} />
          <Route path="/trendforecast" element={<TrendforecastView />} />
          <Route path="/viztemplate" element={<ViztemplateView />} />
          <Route path="/subscription" element={<SubscriptionView />} />
          <Route path="/dimensionmap" element={<DimensionmapView />} />
          <Route path="/qualityscore" element={<QualityscoreView />} />
        </Routes>
      </main>
    </div>
  )
}
