// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import 'store2';
import React, { Suspense, useEffect } from 'react';
import { Navigate, Route, Routes, useLocation } from 'react-router-dom';
// import { Admin } from './admin/Admin';
// import { ViewPage } from './view/ViewPage';
import { currentAccessInfo } from './common/access';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
// import { DashboardListView } from './view/DashboardListView';
// import { SettingsPage } from './view/Settings/SettingsPage';
// import { GroupPage } from './view/Settings/GroupPage';
// import { NamespacePage } from './view/Settings/NamespacePage';
// import View2Page from './view2/ViewPage';
// import Core from './view2/Core';

const FAQ = React.lazy(() => import('./doc/FAQ'));
const Admin = React.lazy(() => import('./admin/Admin'));
const SettingsPage = React.lazy(() => import('./view/Settings/SettingsPage'));
const GroupPage = React.lazy(() => import('./view/Settings/GroupPage'));
const NamespacePage = React.lazy(() => import('./view/Settings/NamespacePage'));
const DashboardListView = React.lazy(() => import('./view/DashboardListView'));

const View2Page = React.lazy(() => import('./view2/ViewPage'));
const Core = React.lazy(() => import('./view2/Core'));
const yAxisSize = 54; // must be synced with .u-legend padding-left

const queryClient = new QueryClient();

function App() {
  const ai = currentAccessInfo();
  return (
    <QueryClientProvider client={queryClient}>
      <Routes>
        <Route path="/" element={<Core />}>
          <Route path="" element={<Navigate to="view" replace={true} />} />
          <Route path="view" element={<View2Page />} />
          <Route path="embed" element={<View2Page />} />
          <Route path="settings/*" element={<SettingsPage adminMode={ai.admin} />}>
            <Route path="group" element={<GroupPage />} />
            <Route path="namespace" element={<NamespacePage />} />
          </Route>
          <Route
            path="dash-list"
            element={
              <Suspense fallback={<div>Loading...</div>}>
                <DashboardListView />
              </Suspense>
            }
          />
          <Route
            path="doc/faq"
            element={
              <Suspense fallback={<div>FAQ Loading...</div>}>
                <FAQ yAxisSize={yAxisSize} />
              </Suspense>
            }
          />
          <Route
            path="admin/*"
            element={
              <Suspense fallback={<div>Loading...</div>}>
                <Admin yAxisSize={yAxisSize} adminMode={ai.admin} />
              </Suspense>
            }
          />
          <Route path="settings/*" element={<SettingsPage adminMode={ai.admin} />}>
            <Route path="group" element={<GroupPage />} />
            <Route path="namespace" element={<NamespacePage />} />
            {/*<Route path="prometheus" element={<PrometheusPage />} />*/}
          </Route>
          <Route path="*" element={<NotFound />} />
        </Route>
        {/*<Route path="/" element={<Navigate to="view" replace={true} />} />*/}
        {/*<Route path="embed" element={<ViewPage embed={true} yAxisSize={yAxisSize} />} />*/}
        {/*<Route path="/" element={<NavbarApp />}>*/}
        {/*  <Route*/}
        {/*    path="doc/faq"*/}
        {/*    element={*/}
        {/*      <Suspense fallback={<div>FAQ Loading...</div>}>*/}
        {/*        <FAQ yAxisSize={yAxisSize} />*/}
        {/*      </Suspense>*/}
        {/*    }*/}
        {/*  />*/}
        {/*  <Route path="admin/*" element={<Admin yAxisSize={yAxisSize} adminMode={ai.admin} />} />*/}
        {/*  <Route path="settings/*" element={<SettingsPage adminMode={ai.admin} />}>*/}
        {/*    <Route path="group" element={<GroupPage />} />*/}
        {/*    <Route path="namespace" element={<NamespacePage />} />*/}
        {/*    /!*<Route path="prometheus" element={<PrometheusPage />} />*!/*/}
        {/*  </Route>*/}

        {/*  <Route path="view" element={<ViewPage yAxisSize={yAxisSize} />} />*/}
        {/*  <Route path="dash-list" element={<DashboardListView />} />*/}
        {/*  <Route path="*" element={<NotFound />} />*/}
        {/*</Route>*/}
      </Routes>
    </QueryClientProvider>
  );
}

// const NavbarApp = function _NavbarApp() {
//   const globalWarning: string = '';
//   return (
//     <div className="d-flex flex-row min-vh-100 position-relative">
//       <HeaderMenu />
//       <div className="flex-grow-1 w-0 d-flex flex-column">
//         {globalWarning !== '' && <div className="alert-warning rounded px-2 py-1">{globalWarning}</div>}
//         <Outlet />
//         <BuildVersion className="text-end text-secondary build-version container-xl pb-3" />
//       </div>
//     </div>
//   );
// };

function NotFound() {
  const location = useLocation();

  useEffect(() => {
    document.title = `404 — StatsHouse`;
  }, []);

  return (
    <div className="container-xl pt-3 pb-3">
      <p className="text-center pt-5">
        <code>{location.pathname}</code> — page not found.
      </p>
    </div>
  );
}

export default App;
