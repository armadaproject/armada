import React from 'react';
import { render, screen } from '@testing-library/react';
import { App }  from './App';
import { JobService } from './services/jobs';
import { LookoutApi } from './openapi';

test('renders learn react link', () => {
  render(<App jobService={new JobService(new LookoutApi())}/>);
  const linkElement = screen.getByText(/Armada Lookout/i);
  expect(linkElement).toBeInTheDocument();
});
